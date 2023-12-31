package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "parquimetro",
	Short: "A small CLI to deal with parquet files",
	Long: `A simple but powerful CLI to deal with parquet files.

parquimetro read ~/path/to/file.parquet (read parquet)
parquimetro schema ~/path/to/file.parquet (show parquet schema)`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().Int64P("threads", "t", 1, "Concurrency Number. Used on commands that reads parquets")
}
