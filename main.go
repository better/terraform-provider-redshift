package main

import (
	"terraform-provider-redshift/redshift"

	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: redshift.Provider,
	})
}
