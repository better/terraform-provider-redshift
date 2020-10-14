package redshift

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

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
