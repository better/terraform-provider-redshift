package main

import (
	"github.com/better/terraform-provider-redshift"

	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: redshift.Provider,
	})
}
