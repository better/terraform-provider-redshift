package redshift

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)


func resourceRedshiftSchema() *schema.Resource {
	return &schema.Resource {
		Create: resourceRedshiftSchemaCreate,
		Read:   resourceRedshiftSchemaRead,
		Update: resourceRedshiftSchemaUpdate,
		Delete: resourceRedshiftSchemaDelete,
		Importer: &schema.ResourceImporter {
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema {
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"owner": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
		},
	}
}

func resourceRedshiftSchemaCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	name := d.Get("name")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftSchemaCreate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	createStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", name)
	if owner, ok := d.GetOk("owner"); ok {
		createStatement = fmt.Sprintf("%s AUTHORIZATION %s", createStatement, owner)
	}
	if _, createErr := tx.Exec(createStatement); createErr != nil {
		log.Println("error | resourceRedshiftSchemaCreate | createErr |", createErr)
		tx.Rollback()
		return createErr
	}

	var id string
	selectQuery := fmt.Sprintf("SELECT oid FROM pg_namespace WHERE nspname = '%s'", name)
	selectErr := tx.QueryRow(selectQuery).Scan(&id)
	if selectErr != nil {
		log.Println("error | resourceRedshiftSchemaCreate | selectErr |", selectErr)
		tx.Rollback()
		return selectErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftSchemaCreate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	d.SetId(id)
	return redshiftSchemaRead(client, d)
}

func resourceRedshiftSchemaRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	return redshiftSchemaRead(client, d)
}

func resourceRedshiftSchemaUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftSchemaUpdate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	if d.HasChange("name") {
		oldName, newName := d.GetChange("name")
		alterNameStatement := fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s", oldName, newName)
		if _, alterNameErr := tx.Exec(alterNameStatement); alterNameErr != nil {
			log.Println("error | resourceRedshiftSchemaUpdate | alterNameErr |", alterNameErr)
			tx.Rollback()
			return alterNameErr
		}
	}

	if d.HasChange("owner") {
		name := d.Get("name")
		owner := d.Get("owner")
		alterOwnerStatement := fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", name, owner)
		if _, alterOwnerErr := tx.Exec(alterOwnerStatement); alterOwnerErr != nil {
			log.Println("error | resourceRedshiftSchemaUpdate | alterOwnerErr |", alterOwnerErr)
			tx.Rollback()
			return alterOwnerErr
		}
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftSchemaUpdate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return redshiftSchemaRead(client, d)
}

func resourceRedshiftSchemaDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	name := d.Get("name")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftSchemaDelete | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	dropStatement := fmt.Sprintf("DROP SCHEMA %s", name)
	if _, dropErr := tx.Exec(dropStatement); dropErr != nil {
		log.Println("error | resourceRedshiftSchemaDelete | dropErr |", dropErr)
		tx.Rollback()
		return dropErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftSchemaDelete | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return nil
}

func redshiftSchemaRead(client *sql.DB, d *schema.ResourceData) error {
	id := d.Id()

	var name string
	var owner string
	selectQuery := fmt.Sprintf(`
		SELECT
			nspname,
			usename
		FROM pg_namespace n
			JOIN pg_user u ON u.usesysid = n.nspowner
		WHERE n.oid =  %s
	`, id)
	selectErr := client.QueryRow(selectQuery).Scan(&name, &owner)

	if selectErr != nil {
		log.Println("error | redshiftSchemaRead | selectErr |", selectErr)
		if selectErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectErr
		}
	}

	d.Set("name", name)
	d.Set("owner", owner)

	return nil
}
