package cmd

import (
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"net/url"
	"os"
	"parquimetro/pkg/schema"

	"github.com/spf13/cobra"
)

// schemaCmd represents the schema command
var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Read schema of a parquet file",
	Long: `Read schema of a parquet file. For example:

parquimetro schema ~/path/to/file.parquet (default json)
parquimetro schema -f go ~/path/to/file/parquet (read parquet as go struct)
parquimetro schema -f go --tags ~/path/to/file.parquet (show struct tags, only valid if format is go)`,
	Run: func(cmd *cobra.Command, args []string) {
		format, _ := cmd.Flags().GetString("format")
		concurrencyCount, _ := cmd.Flags().GetInt64("threads")
		tags, _ := cmd.Flags().GetBool("tags")

		var fileName string
		if len(args) > 0 {
			fileName = args[0]
		} else {
			fmt.Printf("missing file to read.\n\nfor more info try --help\n")
			os.Exit(1)
		}

		uri, err := url.Parse(fileName)

		if err != nil {
			fmt.Printf("Failed to parse file path %s\n", err)
			os.Exit(1)
		}

		fr, err := local.NewLocalFileReader(uri.Path)

		opts := schema.NewSchemaOpts(format, concurrencyCount, tags)

		schema.NewSchema(fr, opts).Show()
	},
}

func init() {
	rootCmd.AddCommand(schemaCmd)

	schemaCmd.Flags().StringP("format", "f", "json", "Format to be printed. json or go struct")
	schemaCmd.Flags().Bool("tags", false, "Show struct tags. Only valid if format is go struct")
}
