package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)


func resourceRedshiftGroup() *schema.Resource {
	return &schema.Resource {
		Create: resourceRedshiftGroupCreate,
		Read:   resourceRedshiftGroupRead,
		Update: resourceRedshiftGroupUpdate,
		Delete: resourceRedshiftGroupDelete,
		Importer: &schema.ResourceImporter {
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema {
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"users": {
				Type:     schema.TypeSet,
				Elem:     &schema.Schema { Type: schema.TypeString },
				Optional: true,
			},
		},
	}
}

func resourceRedshiftGroupCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	name := d.Get("name")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGroupCreate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	createStatement := fmt.Sprintf("CREATE GROUP %s", name)
	if usersSet, ok := d.GetOk("users"); ok {
		users := usersSetToList(usersSet)
		createStatement = fmt.Sprintf("%s WITH USER %s", createStatement, strings.Join(users, ","))
	}
	if _, createErr := tx.Exec(createStatement); createErr != nil {
		log.Println("error | resourceRedshiftGroupCreate | createErr |", createErr)
		tx.Rollback()
		return createErr
	}

	var id string
	selectQuery := fmt.Sprintf("SELECT grosysid FROM pg_group WHERE groname = '%s'", name)
	selectErr := tx.QueryRow(selectQuery).Scan(&id)
	if selectErr != nil {
		log.Println("error | resourceRedshiftGroupCreate | selectErr |", selectErr)
		tx.Rollback()
		return selectErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGroupCreate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	d.SetId(id)
	return redshiftGroupRead(client, d)
}

func resourceRedshiftGroupRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	return redshiftGroupRead(client, d)
}

func resourceRedshiftGroupUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGroupUpdate | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	if d.HasChange("name") {
		oldName, newName := d.GetChange("name")
		alterNameStatement := fmt.Sprintf("ALTER GROUP %s RENAME TO %s", oldName, newName)
		if _, alterNameErr := tx.Exec(alterNameStatement); alterNameErr != nil {
			log.Println("error | resourceRedshiftGroupUpdate | alterNameErr |", alterNameErr)
			tx.Rollback()
			return alterNameErr
		}
	}

	if d.HasChange("users") {
		name := d.Get("name")
		oldUsersSet, newUsersSet := d.GetChange("users")
		oldUsers, newUsers := usersSetToList(oldUsersSet), usersSetToList(newUsersSet)

		if len(oldUsers) > 0 {
			dropUsersStatement := fmt.Sprintf("ALTER GROUP %s DROP USER %s", name, strings.Join(oldUsers, ","))
			if _, dropUsersErr := tx.Exec(dropUsersStatement); dropUsersErr != nil {
				log.Println("error | resourceRedshiftGroupUpdate | dropUsersErr |", dropUsersErr)
				tx.Rollback()
				return dropUsersErr
			}
		}

		if len(newUsers) > 0 {
			addUsersStatement := fmt.Sprintf("ALTER GROUP %s ADD USER %s", name, strings.Join(newUsers, ","))
			if _, addUsersErr := tx.Exec(addUsersStatement); addUsersErr != nil {
				log.Println("error | resourceRedshiftGroupUpdate | addUsersErr |", addUsersErr)
				tx.Rollback()
				return addUsersErr
			}
		}
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGroupUpdate | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return redshiftGroupRead(client, d)
}

func resourceRedshiftGroupDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*Client).db
	name := d.Get("name")

	tx, txBeginErr := client.Begin()
	if txBeginErr != nil {
		log.Println("error | resourceRedshiftGroupDelete | txBeginErr |", txBeginErr)
		return txBeginErr
	}

	dropStatement := fmt.Sprintf("DROP GROUP %s", name)
	if _, dropErr := tx.Exec(dropStatement); dropErr != nil {
		log.Println("error | resourceRedshiftGroupDelete | dropErr |", dropErr)
		tx.Rollback()
		return dropErr
	}

	if txCommitErr := tx.Commit(); txCommitErr != nil {
		log.Println("error | resourceRedshiftGroupDelete | txCommitErr |", txCommitErr)
		return txCommitErr
	}

	return nil
}

func redshiftGroupRead(client *sql.DB, d *schema.ResourceData) error {
	id := d.Id()

	var name string
	selectNameQuery := fmt.Sprintf("SELECT groname FROM pg_group WHERE grosysid = %s", id)
	selectNameErr := client.QueryRow(selectNameQuery).Scan(&name)

	if selectNameErr != nil {
		log.Println("error | redshiftUserRead | selectNameErr", selectNameErr)
		if selectNameErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectNameErr
		}
	}

	var users = []string{}
	selectUsersQuery := fmt.Sprintf(`
		SELECT
			u.usename
		FROM pg_group g
		    JOIN pg_user u ON u.usesysid = ANY(g.grolist)
		WHERE g.grosysid = %s
	`, id)
	rows, selectUsersErr := client.Query(selectUsersQuery)

	if selectUsersErr != nil {
		log.Println("error | redshiftGroupRead | selectUsersErr", selectUsersErr)
		if selectUsersErr == sql.ErrNoRows {
			d.SetId("")
			return nil
		} else {
			return selectUsersErr
		}
	}

	defer rows.Close()
	for rows.Next() {
		var user string
		if selectUsersRowErr := rows.Scan(&user); selectUsersRowErr != nil {
			log.Println("error | redshiftGroupRead | selectUsersRowErr |", selectUsersRowErr)
			return selectUsersRowErr
		}
		users = append(users, user)
	}

	d.Set("name", name)
	d.Set("users", users)
	return nil
}

func usersSetToList(usersSet interface{}) []string {
	var users []string
	for _, v := range usersSet.(*schema.Set).List() {
		users = append(users, v.(string))
	}
	return users
}
