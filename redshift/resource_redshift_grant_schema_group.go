package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)


func resourceRedshiftGrantSchemaGroup() *schema.Resource {
	return &schema.Resource {
		Create: resourceRedshiftGrantSchemaGroupCreate,
		Read:   resourceRedshiftGrantSchemaGroupRead,
		Update: resourceRedshiftGrantSchemaGroupCreate,
		Delete: resourceRedshiftGrantSchemaGroupDelete,
		Importer: &schema.ResourceImporter {
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema {
			"group": {
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

func resourceRedshiftGrantSchemaGroupCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	group := d.Get("group")
	schema := d.Get("schema")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupCreate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeStatement := fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM GROUP %s CASCADE", schema, group)
	if _, revokeErr := tx.Exec(revokeStatement); revokeErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupCreate | revokeErr |", revokeErr)
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
		log.Println("error | resourceRedshiftGrantSchemaGroupCreate | len(grants) == 0 | Must have at least 1 privilege")
		tx.Rollback()
		return fmt.Errorf("Must have at least 1 privilege")
	}

	grantStatement := fmt.Sprintf("GRANT %s ON SCHEMA %s TO GROUP %s", strings.Join(grants, ","), schema, group)
	if _, grantErr := tx.Exec(grantStatement); grantErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupCreate | grantErr |", grantErr)
		tx.Rollback()
		return grantErr
	}

	var groupId string
	selectUserQuery := fmt.Sprintf("SELECT grosysid FROM pg_group WHERE groname = '%s'", group)
	selectUserErr := tx.QueryRow(selectUserQuery).Scan(&groupId)
	if selectUserErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupCreate | selectUserErr |", selectUserErr)
		tx.Rollback()
		return selectUserErr
	}

	var schemaId string
	selectSchemaQuery := fmt.Sprintf("SELECT oid FROM pg_namespace WHERE nspname = '%s'", schema)
	selectSchemaErr := tx.QueryRow(selectSchemaQuery).Scan(&schemaId)
	if selectSchemaErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupCreate | selectSchemaErr |", selectSchemaErr)
		tx.Rollback()
		return selectSchemaErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupCreate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	id := fmt.Sprintf("%s-%s", groupId, schemaId)
	d.SetId(id)
	return redshiftGrantSchemaGroupRead(client, d)
}

func resourceRedshiftGrantSchemaGroupRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	return redshiftGrantSchemaGroupRead(client, d)
}

func resourceRedshiftGrantSchemaGroupDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	group := d.Get("group")
	schema := d.Get("schema")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupDelete | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeStatement := fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM GROUP %s CASCADE", schema, group)
	if _, revokeErr := tx.Exec(revokeStatement); revokeErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupDelete | revokeErr |", revokeErr)
		tx.Rollback()
		return revokeErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantSchemaGroupDelete | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return nil
}

func redshiftGrantSchemaGroupRead(client *sql.DB, d *schema.ResourceData) error {
	id := d.Id()
	parts := strings.SplitN(id, "-", 2)
	groupId, schemaId := parts[0], parts[1]

	var group string
	var schema string
	var usagePrivilege bool
	var createPrivilege bool
	selectQuery := fmt.Sprintf(`
		WITH g AS (SELECT groname FROM pg_group WHERE grosysid = %s)
		SELECT
		    g.groname,
		    n.nspname,
		    regexp_replace('|' + array_to_string(n.nspacl, '|') + '|', '.*\\bgroup ' + g.groname + '\\b=([^\\/]*)\\/.*', '$1') LIKE '%%U%%' AS usage,
		    regexp_replace('|' + array_to_string(n.nspacl, '|') + '|', '.*\\bgroup ' + g.groname + '\\b=([^\\/]*)\\/.*', '$1') LIKE '%%C%%' AS create
		FROM pg_namespace n, g
		WHERE n.oid = %s
		    AND '|' + array_to_string(n.nspacl, '|') + '|' LIKE '%%|group ' + g.groname + '=%%'
	`, groupId, schemaId)
	selectErr := client.QueryRow(selectQuery).Scan(&group, &schema, &usagePrivilege, &createPrivilege)

	if selectErr != nil {
		log.Println("error | redshiftGrantSchemaGroupRead |", selectErr)
		if selectErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectErr
		}
	}

	d.Set("group", group)
	d.Set("schema", schema)
	d.Set("usage", usagePrivilege)
	d.Set("create", createPrivilege)

	return nil
}
