package redshift

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)


func resourceRedshiftUser() *schema.Resource {
	return &schema.Resource {
		Create: resourceRedshiftUserCreate,
		Read:   resourceRedshiftUserRead,
		Update: resourceRedshiftUserUpdate,
		Delete: resourceRedshiftUserDelete,
		Importer: &schema.ResourceImporter {
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema {
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"password": {
				Type:      schema.TypeString,
				Required:  true,
				Sensitive: true,
			},
		},
	}
}

func resourceRedshiftUserCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	name := d.Get("name")
	password := d.Get("password")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftUserCreate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	createStatement := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", name, password)
	if _, createErr := tx.Exec(createStatement); createErr != nil {
		log.Println("error | resourceRedshiftUserCreate | createErr |", createErr)
		tx.Rollback()
		return createErr
	}

	var id string
	selectQuery := fmt.Sprintf("SELECT usesysid FROM pg_user WHERE usename = '%s'", name)
	selectErr := tx.QueryRow(selectQuery).Scan(&id)
	if selectErr != nil {
		log.Println("error | resourceRedshiftUserCreate | selectErr |", selectErr)
		tx.Rollback()
		return selectErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftUserCreate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	d.SetId(id)
	return redshiftUserRead(client, d)
}

func resourceRedshiftUserRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	return redshiftUserRead(client, d)
}

func resourceRedshiftUserUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftUserUpdate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	if d.HasChange("name") {
		oldName, newName := d.GetChange("name")
		alterNameStatement := fmt.Sprintf("ALTER USER %s RENAME TO %s", oldName, newName)
		if _, alterNameErr := tx.Exec(alterNameStatement); alterNameErr != nil {
			log.Println("error | resourceRedshiftUserUpdate | alterNameErr |", alterNameErr)
			tx.Rollback()
			return alterNameErr
		}
	}

	if d.HasChange("password") {
		name := d.Get("name")
		password := d.Get("password")
		alterPasswordStatement := fmt.Sprintf("ALTER USER %s PASSWORD '%s'", name, password)
		if _, alterPasswordrErr := tx.Exec(alterPasswordStatement); alterPasswordrErr != nil {
			log.Println("error | resourceRedshiftUserUpdate | alterPasswordrErr |", alterPasswordrErr)
			tx.Rollback()
			return alterPasswordrErr
		}
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftUserUpdate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return redshiftUserRead(client, d)
}

func resourceRedshiftUserDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	name := d.Get("name")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftUserDelete | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	dropStatement := fmt.Sprintf("DROP USER %s", name)
	if _, dropErr := tx.Exec(dropStatement); dropErr != nil {
		log.Println("error | resourceRedshiftUserDelete | dropErr |", dropErr)
		tx.Rollback()
		return dropErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftUserDelete | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return nil
}

func redshiftUserRead(client *sql.DB, d *schema.ResourceData) error {
	id := d.Id()

	var name string
	selectQuery := fmt.Sprintf("SELECT usename FROM pg_user WHERE usesysid = %s", id)
	selectErr := client.QueryRow(selectQuery).Scan(&name)

	if selectErr != nil {
		log.Println("error | redshiftUserRead | selectErr", selectErr)
		if selectErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectErr
		}
	}

	d.Set("name", name)

	return nil
}
