package redshift

import (
    "log"

    "github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Provider() *schema.Provider {
    return &schema.Provider {
        Schema: map[string]*schema.Schema {
            "host": {
                Type:        schema.TypeString,
                Description: "host",
                Required:    true,
            },
            "user": {
                Type:        schema.TypeString,
                Description: "user",
                Required:    true,
            },
            "password": {
                Type:        schema.TypeString,
                Description: "password",
                Required:    true,
                Sensitive:   true,
            },
            "port": {
                Type:        schema.TypeString,
                Description: "port",
                Optional:    true,
                Default:     "5439",
            },
            "ssl_mode": {
                Type:        schema.TypeString,
                Description: "ssl_mode",
                Optional:    true,
                Default:     "require", //  require, disable, verify-ca, verify-full
            },
            "database": {
                Type:        schema.TypeString,
                Description: "database",
                Required:    true,
            },
        },
        ResourcesMap: map[string]*schema.Resource {
            "redshift_grant_table_group":   resourceRedshiftGrantTableGroup(),
            "redshift_grant_table_user":    resourceRedshiftGrantTableUser(),
            "redshift_grant_schema_group":  resourceRedshiftGrantSchemaGroup(),
            "redshift_grant_schema_user":   resourceRedshiftGrantSchemaUser(),
            "redshift_group":               resourceRedshiftGroup(),
            "redshift_schema":              resourceRedshiftSchema(),
            "redshift_user":                resourceRedshiftUser(),
            "redshift_user_password":       resourceRedshiftUserPassword(),
            "redshift_user_password_association": resourceRedshiftUserPasswordAssociation(),
        },
        ConfigureFunc: providerConfigure,
    }
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
    config := Config{
        host:     d.Get("host").(string),
        user:     d.Get("user").(string),
        password: d.Get("password").(string),
        port:     d.Get("port").(string),
        sslMode:  d.Get("ssl_mode").(string),
        database: d.Get("database").(string),
    }

    log.Println("info | provider | providerConfigure | initializing redshift client")
    client, err := config.Client()
    if err != nil {
        return nil, err
    }

    db := client.db

    if err = db.Ping(); err != nil {
        log.Println("error | provider | providerConfigure | %v", err)
        return nil, err
    }

    return client, nil
}
