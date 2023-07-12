package cmd

import (
	"fmt"
	"github.com/otaviohenrique/parquimetro/pkg/reader"
	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"net/url"
	"os"
)

// sizeCmd represents the size command
var sizeCmd = &cobra.Command{
	Use:   "size",
	Short: "Calculate parquet size",
	Long:  `Calculate parquet size`,
	Run: func(cmd *cobra.Command, args []string) {
		showCompressed, _ := cmd.Flags().GetBool("compressed")
		concurrencyCount, _ := cmd.Flags().GetInt64("threads")

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

		if err != nil {
			fmt.Printf("Error creating file reader %s\n", err)
		}

		parquetSize := reader.NewReader(fr, concurrencyCount)

		if showCompressed {
			println(parquetSize.CompressedSize())
		} else {
			println(parquetSize.UncompressedSize())
		}
	},
}

func init() {
	rootCmd.AddCommand(sizeCmd)

	sizeCmd.Flags().Bool("uncompressed", true, "Show show uncompressed size")
	sizeCmd.Flags().Bool("compressed", false, "Show compressed size")
}
