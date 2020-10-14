package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)


func resourceRedshiftGrantTableGroup() *schema.Resource {
	return &schema.Resource {
		Create: resourceRedshiftGrantTableGroupCreate,
		Read:   resourceRedshiftGrantTableGroupRead,
		Update: resourceRedshiftGrantTableGroupCreate,
		Delete: resourceRedshiftGrantTableGroupDelete,
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

func resourceRedshiftGrantTableGroupCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	group := d.Get("group")
	schema := d.Get("schema")
	owner := d.Get("owner")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeGrantStatement := fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM GROUP %s CASCADE", schema, group)
	if _, revokeGrantErr := tx.Exec(revokeGrantStatement); revokeGrantErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | revokeGrantErr |", revokeGrantErr)
		tx.Rollback()
		return revokeGrantErr
	}

	revokeDefaultStatement := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA %s REVOKE ALL ON TABLES FROM GROUP %s", owner, schema, group)
	if _, revokeDefaultErr := tx.Exec(revokeDefaultStatement); revokeDefaultErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | revokeDefaultErr |", revokeDefaultErr)
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
		log.Println("error | resourceRedshiftGrantTableGroupCreate | len(grants) == 0 | Must have at least 1 privilege")
		tx.Rollback()
		return fmt.Errorf("Must have at least 1 privilege")
	}

	grantGrantStatement := fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA %s TO GROUP %s", strings.Join(grants, ","), schema, group)
	if _, grantGrantErr := tx.Exec(grantGrantStatement); grantGrantErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | grantGrantErr |", grantGrantErr)
		tx.Rollback()
		return grantGrantErr
	}

	grantDefaultStatement := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA %s GRANT %s ON TABLES TO GROUP %s", owner, schema, strings.Join(grants, ","), group)
	if _, grantDefaultErr := tx.Exec(grantDefaultStatement); grantDefaultErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | grantDefaultErr |", grantDefaultErr)
		tx.Rollback()
		return grantDefaultErr
	}

	var groupId string
	selectUserQuery := fmt.Sprintf("SELECT grosysid FROM pg_group WHERE groname = '%s'", group)
	selectUserErr := tx.QueryRow(selectUserQuery).Scan(&groupId)
	if selectUserErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | selectUserErr |", selectUserErr)
		tx.Rollback()
		return selectUserErr
	}

	var schemaId string
	selectSchemaQuery := fmt.Sprintf("SELECT oid FROM pg_namespace WHERE nspname = '%s'", schema)
	selectSchemaErr := tx.QueryRow(selectSchemaQuery).Scan(&schemaId)
	if selectSchemaErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | selectSchemaErr |", selectSchemaErr)
		tx.Rollback()
		return selectSchemaErr
	}

	var ownerId string
	selectOwnerQuery := fmt.Sprintf("SELECT usesysid FROM pg_user WHERE usename = '%s'", owner)
	selectOwnerErr := tx.QueryRow(selectOwnerQuery).Scan(&ownerId)
	if selectOwnerErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | selectOwnerErr |", selectOwnerErr)
		tx.Rollback()
		return selectOwnerErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupCreate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	id := fmt.Sprintf("%s-%s-%s", groupId, schemaId, ownerId)
	d.SetId(id)
	return redshiftGrantTableGroupRead(client, d)
}

func resourceRedshiftGrantTableGroupRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	return redshiftGrantTableGroupRead(client, d)
}

func resourceRedshiftGrantTableGroupDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	group := d.Get("group")
	schema := d.Get("schema")
	owner := d.Get("owner")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupDelete | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	revokeGrantStatement := fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM GROUP %s CASCADE", schema, group)
	if _, revokeGrantErr := tx.Exec(revokeGrantStatement); revokeGrantErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupDelete | revokeGrantErr |", revokeGrantErr)
		tx.Rollback()
		return revokeGrantErr
	}

	revokeDefaultStatement := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA %s REVOKE ALL ON TABLES FROM GROUP %s", owner, schema, group)
	if _, revokeDefaultErr := tx.Exec(revokeDefaultStatement); revokeDefaultErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupDelete | revokeDefaultErr |", revokeDefaultErr)
		tx.Rollback()
		return revokeDefaultErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGrantTableGroupDelete | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return nil
}

func redshiftGrantTableGroupRead(client *sql.DB, d *schema.ResourceData) error {
	id := d.Id()
	parts := strings.SplitN(id, "-", 3)
	groupId, schemaId, ownerId := parts[0], parts[1], parts[2]

	var group string
	var schema string
	var owner string
	var selectPrivilege bool
	var insertPrivilege bool
	var updatePrivilege bool
	var deletePrivilege bool
	var referencesPrivilege bool
	selectQuery := fmt.Sprintf(`
		WITH
			g AS (SELECT groname FROM pg_group WHERE grosysid = %s),
			o AS (SELECT usename FROM pg_user WHERE usesysid = %s)
		SELECT
		    g.groname,
		    n.nspname,
		    o.usename,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\bgroup ' + g.groname + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%r%%' AS select,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\bgroup ' + g.groname + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%a%%' AS insert,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\bgroup ' + g.groname + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%w%%' AS update,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\bgroup ' + g.groname + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%d%%' AS delete,
		    regexp_replace('|' + array_to_string(d.defaclacl, '|') + '|', '.*\\bgroup ' + g.groname + '\\b=([^\\/]*)\\/\\b' + o.usename + '\\b.*', '$1') LIKE '%%x%%' AS references
		FROM g, o, pg_default_acl d
		    JOIN pg_namespace n ON n.oid = d.defaclnamespace
		WHERE n.oid = %s
		  AND '|' + array_to_string(d.defaclacl, '|') + '|' LIKE '%%|group ' + g.groname + '=%%'
		  AND '|' + array_to_string(d.defaclacl, '|') + '|' LIKE '%%/' + o.usename + '|%%'
	`, groupId, ownerId, schemaId)
	selectErr := client.QueryRow(selectQuery).Scan(&group, &schema, &owner, &selectPrivilege, &insertPrivilege, &updatePrivilege, &deletePrivilege, &referencesPrivilege)

	if selectErr != nil {
		log.Println("error | redshiftGrantTableGroupRead |", selectErr)
		if selectErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectErr
		}
	}

	d.Set("group", group)
	d.Set("schema", schema)
	d.Set("owner", owner)
	d.Set("select", selectPrivilege)
	d.Set("insert", insertPrivilege)
	d.Set("update", updatePrivilege)
	d.Set("delete", deletePrivilege)
	d.Set("references", referencesPrivilege)

	return nil
}
