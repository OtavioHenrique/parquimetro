package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "parquimetro",
	Short: "A small CLI to deal with parquet files",
	Long:  `A simple but powerful CLI to deal with parquet files.`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
