# Redshift Terraform Provider
Fork off:
* [frankfarrell/terraform-provider-redshift](https://github.com/frankfarrell/terraform-provider-redshift)
* [terraform-providers/terraform-provider-postgresql](https://github.com/terraform-providers/terraform-provider-postgresql)

Uses Terraform to manage AWS Redshift schemas, users, groups and permissions.

Does not support database and table formation.

## Requirements

* [Terraform](https://www.terraform.io/downloads.html) 0.13.x
* [Go](https://golang.org/doc/install) 1.15 (to build the provider plugin)

## Local Install
Make sure you have `go` installed and  `$GOPATH` set.
Clone the repo to: `$GOPATH/src/github.com/terraform-providers/` and `go install` then move the provider to the terraform plugin directory

#### Building the provider:
```
$ mkdir -p $GOPATH/src/github.com/terraform-providers; cd $GOPATH/src/github.com/terraform-providers
$ git clone git@github.com:better/terraform-provider-redshift.git
$ cd $GOPATH/src/github.com/terraform-providers/terraform-provider-redshift
$ go install
```

#### Using the provider
```
$ mkdir -p ~/.terraform.d/plugins/{organization}/{team_name}/redshift/1.0.0/darwin_amd64/
$ cp $GOPATH/bin/terraform-provider-redshift ~/.terraform.d/plugins/{organization}/{team_name}/redshift/1.0.0/darwin_amd64/
```

main.tf
```
terraform {
  required_providers {
    redshift = {
      version = "1.0.0"
      source = "{organization}/{team_name}/redshift"
    }
  }
}


provider redshift {
  host = "localhost"
  user = "root"
  password = "password"
  database = "database"
  port = 5439
}


# create users

resource redshift_user "tf_test__user" {
  name = "tf_test__user"
  password = "password_goes_here"
}

resource redshift_user "tf_test__user2" {
  name = "tf_test__user2"
  password = "password_goes_here"
}


# create test schema

resource redshift_schema "test_schema" {
  name = "test_schema"
}


# create groups and assign users to groups

resource redshift_group "test_schema__rw" {
  name = "test_schema__rw"
  users = [redshift_user.tf__test_user.name]
}

resource redshift_group "test_schema__r" {
  name = "test_schema__r"
  users = [redshift_user.tf__test_user2.name]
}


# create schema permissions and schema table grants/default privileges and assign them to groups
# "owner" defines the target user in ALTER DEFAULT PRIVILEGES

resource redshift_grant_schema_group "tf__test_schema__rw__test_schema" {
  group = redshift_group.test_schema__rw.name
  schema = redshift_schema.test_schema.name
  usage = true
  create = true
}

resource redshift_grant_schema_group "tf__test_schema__r__test_schema" {
  group = redshift_group.test_schema__r.name
  schema = redshift_schema.test_schema.name
  usage = true
}

resource redshift_grant_table_group "tf__test_schema__rw__test_schema__tf__test_user" {
  group = redshift_group.test_schema__rw.name
  schema = redshift_schema.test_schema.name
  owner = redshift_user.tf__test_user.name
  select = true
  insert = true
  update = true
  delete = true
  references = true

  depends_on = [
    redshift_grant_schema_group.tf__test_schema__rw__test_schema,
  ]
}

resource redshift_grant_table_group "tf__test_schema__r__test_schema__tf__test_user" {
  group = redshift_group.test_schema__r.name
  schema = redshift_schema.test_schema.name
  owner = redshift_user.tf__test_user.name
  select = true

  depends_on = [
    redshift_grant_schema_group.tf__test_schema__r__test_schema,
  ]
}
```

Terraform CLI
```
terraform plan
terraform apply -parallelism 1
```
#### Importing already existing resources

main.tf
```
resource redshift_user "tf_test__user" {
  name = "tf_test__user"
  password = "password_goes_here"
}

resource redshift_group "tf_test__group" {
  name = "tf_test__group"
}

resource redshift_schema "tf_test__schema" {
  name = "tf_test__schema"
}
```

Terraform CLI
```
terraform import redshift_user.tf_test__user <usesysid>
terraform import redshift_group.tf_test__group <grosysid>
terraform import redshift_schema.tf_test__schema <oid>
```
