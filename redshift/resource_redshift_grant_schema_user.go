package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)


func resourceRedshiftGrantSchemaUser() *schema.Resource {
	return &schema.Resource {
		Create: resourceRedshiftGrantSchemaUserCreate,
		Read:   resourceRedshiftGrantSchemaUserRead,
		Update: resourceRedshiftGrantSchemaUserCreate,
		Delete: resourceRedshiftGrantSchemaUserDelete,
		Importer: &schema.ResourceImporter {
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema {
			"user": {
				Type:     schema.TypeString,
				Required: true,
			},
			"schema": {
				Type:     schema.TypeString,
				Required: true,
			},
			"usage": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"create": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
		},
	}
}

func resourceRedshiftGrantSchemaUserCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	user := d.Get("user")
	schema := d.Get("schema")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserCreate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeStatement := fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM %s CASCADE", schema, user)
	if _, revokeErr := tx.Exec(revokeStatement); revokeErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserCreate | revokeErr |", revokeErr)
		tx.Rollback()
		return revokeErr
	}

	var grants []string
	if v, ok := d.GetOk("usage"); ok && v.(bool) {
		grants = append(grants, "USAGE")
	}
	if v, ok := d.GetOk("create"); ok && v.(bool) {
		grants = append(grants, "CREATE")
	}

	if len(grants) == 0 {
		log.Println("error | resourceRedshiftGrantSchemaUserCreate | len(grants) == 0 | Must have at least 1 privilege")
		tx.Rollback()
		return fmt.Errorf("Must have at least 1 privilege")
	}

	grantStatement := fmt.Sprintf("GRANT %s ON SCHEMA %s TO %s", strings.Join(grants, ","), schema, user)
	if _, grantErr := tx.Exec(grantStatement); grantErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserCreate | grantErr |", grantErr)
		tx.Rollback()
		return grantErr
	}

	var userId string
	selectUserQuery := fmt.Sprintf("SELECT usesysid FROM pg_user WHERE usename = '%s'", user)
	selectUserErr := tx.QueryRow(selectUserQuery).Scan(&userId)
	if selectUserErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserCreate | selectUserErr |", selectUserErr)
		tx.Rollback()
		return selectUserErr
	}

	var schemaId string
	selectSchemaQuery := fmt.Sprintf("SELECT oid FROM pg_namespace WHERE nspname = '%s'", schema)
	selectSchemaErr := tx.QueryRow(selectSchemaQuery).Scan(&schemaId)
	if selectSchemaErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserCreate | selectSchemaErr |", selectSchemaErr)
		tx.Rollback()
		return selectSchemaErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserCreate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	id := fmt.Sprintf("%s-%s", userId, schemaId)
	d.SetId(id)
	return redshiftGrantSchemaUserRead(client, d)
}

func resourceRedshiftGrantSchemaUserRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	return redshiftGrantSchemaUserRead(client, d)
}

func resourceRedshiftGrantSchemaUserDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	user := d.Get("user")
	schema := d.Get("schema")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserDelete | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeStatement := fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM %s CASCADE", schema, user)
	if _, revokeErr := tx.Exec(revokeStatement); revokeErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserDelete | revokeErr |", revokeErr)
		tx.Rollback()
		return revokeErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaUserDelete | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return nil
}

func redshiftGrantSchemaUserRead(client *sql.DB, d *schema.ResourceData) error {
	id := d.Id()
	parts := strings.SplitN(id, "-", 2)
	userId, schemaId := parts[0], parts[1]

	var user string
	var schema string
	var usagePrivilege bool
	var createPrivilege bool
	selectQuery := fmt.Sprintf(`
		WITH u AS (SELECT usename FROM pg_user WHERE usesysid = %s)
		SELECT
		    u.usename,
		    n.nspname,
		    regexp_replace('|' + array_to_string(n.nspacl, '|') + '|', '.*\\b' + u.usename + '\\b=([^\\/]*)\\/.*', '$1') LIKE '%%U%%' AS usage,
		    regexp_replace('|' + array_to_string(n.nspacl, '|') + '|', '.*\\b' + u.usename + '\\b=([^\\/]*)\\/.*', '$1') LIKE '%%C%%' AS create
		FROM pg_namespace n, u
		WHERE n.oid = %s
		    AND '|' + array_to_string(n.nspacl, '|') + '|' LIKE '%%|' + u.usename + '=%%'
	`, userId, schemaId)
	selectErr := client.QueryRow(selectQuery).Scan(&user, &schema, &usagePrivilege, &createPrivilege)

	if selectErr != nil {
		log.Println("error | redshiftGrantSchemaUserRead |", selectErr)
		if selectErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectErr
		}
	}

	d.Set("user", user)
	d.Set("schema", schema)
	d.Set("usage", usagePrivilege)
	d.Set("create", createPrivilege)

	return nil
}
