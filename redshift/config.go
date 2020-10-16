package redshift

import (
	"database/sql"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	_ "github.com/lib/pq"
)

func Compact(d []string) []string {
	r := make([]string, 0)

	for _, v := range d {
		if v != "" {
			r = append(r, v)
		}
	}

	return r
}

func getSession() *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err == nil {
		return sess
	} else {
		fmt.Println(err.Error())
		return nil
	}
}

type Config struct {
	host      string
	user     string
	password string
	port     string
	database string
	sslMode  string
}

type Client struct {
	config Config
	db     *sql.DB
}

func (c *Config) Client() (*Client, error) {
	connInfo := fmt.Sprintf("sslmode=%v user=%v password=%v host=%v port=%v dbname=%v",
		c.sslMode,
		c.user,
		c.password,
		c.host,
		c.port,
		c.database)

	db, err := sql.Open("postgres", connInfo)
	if err != nil {
		db.Close()
		return nil, err
	}

	client := Client{
		config: *c,
		db:     db,
	}

	return &client, nil
}
