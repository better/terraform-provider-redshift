package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)


func resourceRedshiftGrantTableUser() *schema.Resource {
	return &schema.Resource {
		Create: resourceRedshiftGrantTableUserCreate,
		Read:   resourceRedshiftGrantTableUserRead,
		Update: resourceRedshiftGrantTableUserCreate,
		Delete: resourceRedshiftGrantTableUserDelete,
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
			"owner": {
				Type:     schema.TypeString,
				Required: true,
			},
			"select": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"insert": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"update": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"delete": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"references": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
		},
	}
}

func resourceRedshiftGrantTableUserCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	user := d.Get("user")
	schema := d.Get("schema")
	owner := d.Get("owner")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeGrantStatement := fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s CASCADE", schema, user)
	if _, revokeGrantErr := tx.Exec(revokeGrantStatement); revokeGrantErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | revokeGrantErr |", revokeGrantErr)
		tx.Rollback()
		return revokeGrantErr
	}

	revokeDefaultStatement := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA %s REVOKE ALL ON TABLES FROM %s", owner, schema, user)
	if _, revokeDefaultErr := tx.Exec(revokeDefaultStatement); revokeDefaultErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | revokeDefaultErr |", revokeDefaultErr)
		tx.Rollback()
		return revokeDefaultErr
	}

	var grants []string
	if v, ok := d.GetOk("select"); ok && v.(bool) {
		grants = append(grants, "SELECT")
	}
	if v, ok := d.GetOk("insert"); ok && v.(bool) {
		grants = append(grants, "INSERT")
	}
	if v, ok := d.GetOk("update"); ok && v.(bool) {
		grants = append(grants, "UPDATE")
	}
	if v, ok := d.GetOk("delete"); ok && v.(bool) {
		grants = append(grants, "DELETE")
	}
	if v, ok := d.GetOk("references"); ok && v.(bool) {
		grants = append(grants, "REFERENCES")
	}

	if len(grants) == 0 {
		log.Println("error | resourceRedshiftGrantTableUserCreate | len(grants) == 0 | Must have at least 1 privilege")
		tx.Rollback()
		return fmt.Errorf("Must have at least 1 privilege")
	}

	grantGrantStatement := fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA %s TO %s", strings.Join(grants, ","), schema, user)
	if _, grantGrantErr := tx.Exec(grantGrantStatement); grantGrantErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | grantGrantErr |", grantGrantErr)
		tx.Rollback()
		return grantGrantErr
	}

	grantDefaultStatement := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA %s GRANT %s ON TABLES TO %s", owner, schema, strings.Join(grants, ","), user)
	if _, grantDefaultErr := tx.Exec(grantDefaultStatement); grantDefaultErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | grantDefaultErr |", grantDefaultErr)
		tx.Rollback()
		return grantDefaultErr
	}

	var userId string
	selectUserQuery := fmt.Sprintf("SELECT usesysid FROM pg_user WHERE usename = '%s'", user)
	selectUserErr := tx.QueryRow(selectUserQuery).Scan(&userId)
	if selectUserErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | selectUserErr |", selectUserErr)
		tx.Rollback()
		return selectUserErr
	}

	var schemaId string
	selectSchemaQuery := fmt.Sprintf("SELECT oid FROM pg_namespace WHERE nspname = '%s'", schema)
	selectSchemaErr := tx.QueryRow(selectSchemaQuery).Scan(&schemaId)
	if selectSchemaErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | selectSchemaErr |", selectSchemaErr)
		tx.Rollback()
		return selectSchemaErr
	}

	var ownerId string
	selectOwnerQuery := fmt.Sprintf("SELECT usesysid FROM pg_user WHERE usename = '%s'", owner)
	selectOwnerErr := tx.QueryRow(selectOwnerQuery).Scan(&ownerId)
	if selectOwnerErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | selectOwnerErr |", selectOwnerErr)
		tx.Rollback()
		return selectOwnerErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserCreate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	id := fmt.Sprintf("%s-%s-%s", userId, schemaId, ownerId)
	d.SetId(id)
	return redshiftGrantTableUserRead(client, d)
}

func resourceRedshiftGrantTableUserRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	return redshiftGrantTableUserRead(client, d)
}

func resourceRedshiftGrantTableUserDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	user := d.Get("user")
	schema := d.Get("schema")
	owner := d.Get("owner")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserDelete | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeGrantStatement := fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s CASCADE", schema, user)
	if _, revokeGrantErr := tx.Exec(revokeGrantStatement); revokeGrantErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserDelete | revokeGrantErr |", revokeGrantErr)
		tx.Rollback()
		return revokeGrantErr
	}

	revokeDefaultStatement := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA %s REVOKE ALL ON TABLES FROM %s", owner, schema, user)
	if _, revokeDefaultErr := tx.Exec(revokeDefaultStatement); revokeDefaultErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserDelete | revokeDefaultErr |", revokeDefaultErr)
		tx.Rollback()
		return revokeDefaultErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantTableUserDelete | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return nil
}

func redshiftGrantTableUserRead(client *sql.DB, d *schema.ResourceData) error {
	id := d.Id()
	parts := strings.SplitN(id, "-", 3)
	userId, schemaId, ownerId := parts[0], parts[1], parts[2]

	var user string
	var schema string
	var owner string
	var selectPrivilege bool
	var insertPrivilege bool
	var updatePrivilege bool
	var deletePrivilege bool
	var referencesPrivilege bool
	selectQuery := fmt.Sprintf(`
		WITH
			u AS (SELECT usename FROM pg_user WHERE usesysid = %s),
			o AS (SELECT usename FROM pg_user WHERE usesysid = %s)
		SELECT
		    u.usename,
		    n.nspname,
		    o.usename,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\b' + u.usename + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%r%%' AS select,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\b' + u.usename + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%a%%' AS insert,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\b' + u.usename + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%w%%' AS update,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\b' + u.usename + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%d%%' AS delete,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\b' + u.usename + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%x%%' AS references
		FROM u, o, pg_default_acl d
		    JOIN pg_namespace n ON n.oid = d.defaclnamespace
		WHERE n.oid = %s
		  AND '|' + array_to_string(d.defaclacl, '|') + '|' LIKE '%%|' + u.usename + '=%%'
		  AND '|' + array_to_string(d.defaclacl, '|') + '|' LIKE '%%/' + o.usename + '|%%'
	`, userId, ownerId, schemaId)
	selectErr := client.QueryRow(selectQuery).Scan(&user, &schema, &owner, &selectPrivilege, &insertPrivilege, &updatePrivilege, &deletePrivilege, &referencesPrivilege)

	if selectErr != nil {
		log.Println("error | redshiftGrantTableUserRead |", selectErr)
		if selectErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectErr
		}
	}

	d.Set("user", user)
	d.Set("schema", schema)
	d.Set("owner", owner)
	d.Set("select", selectPrivilege)
	d.Set("insert", insertPrivilege)
	d.Set("update", updatePrivilege)
	d.Set("delete", deletePrivilege)
	d.Set("references", referencesPrivilege)

	return nil
}
