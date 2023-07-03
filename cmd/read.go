package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"net/url"
	"os"
	"parquimetro/pkg/parquet_reader"
)

var readCmd = &cobra.Command{
	Use:   "read",
	Short: "Read parquet file",
	Long: `Read parquet file. Example:

parquimetro read /Users/name/Downloads/parquet.parquet
parquimetro read ~/Downloads/parquet.parquet`,
	Run: func(cmd *cobra.Command, args []string) {
		readCount, _ := cmd.Flags().GetInt64("count")
		concurrencyCount, _ := cmd.Flags().GetInt64("threads")
		skipCount, _ := cmd.Flags().GetInt64("skip")

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

		opts := parquet_reader.NewParquetReaderOpts(readCount, skipCount, concurrencyCount)

		parquet_reader.NewParquetReader(fr, opts).Read()
	},
}

func init() {
	rootCmd.AddCommand(readCmd)

	readCmd.Flags().Int64P("count", "c", 25, "Number of rows to be printed")
	readCmd.Flags().Int64P("threads", "t", 1, "Concurrency Number")
	readCmd.Flags().Int64P("skip", "s", 0, "Number of rows to be skipped (from the beginning).")
}
