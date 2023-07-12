package cmd

import (
	"fmt"
	"github.com/otaviohenrique/parquimetro/pkg/reader"
	"github.com/otaviohenrique/parquimetro/pkg/size"
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
		showUncompressed, _ := cmd.Flags().GetBool("uncompressed")
		pretty, _ := cmd.Flags().GetBool("pretty")
		format, _ := cmd.Flags().GetString("format")
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
			if pretty == true {

			}
			fmt.Printf("Error creating file reader %s\n", err)
		}

		parquetSize := reader.NewReader(fr, concurrencyCount)

		if showCompressed {
			compressedSize := parquetSize.CompressedSize()

			if pretty == true {
				fmt.Printf("Compressed: %s\n", size.PrettyFormatSize(compressedSize))
			} else {
				formattedSize, err := size.FormatBytes(float64(compressedSize), format)

				if err != nil {
					fmt.Printf("Error %s\n", err)
				}

				fmt.Printf("Compressed: %s\n", formattedSize)
			}
		}

		if showUncompressed {
			uncompressedSize := parquetSize.UncompressedSize()

			if pretty == true {
				fmt.Printf("Uncompressed: %s\n", size.PrettyFormatSize(uncompressedSize))
			} else {
				formattedSize, err := size.FormatBytes(float64(uncompressedSize), format)

				if err != nil {
					fmt.Printf("Error %s\n", err)
				}

				fmt.Printf("Uncompressed: %s\n", formattedSize)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(sizeCmd)

	sizeCmd.Flags().Bool("uncompressed", true, "Show show uncompressed size")
	sizeCmd.Flags().Bool("compressed", false, "Show compressed size")
	sizeCmd.Flags().Bool("pretty", true, "Show pretty size (It will use the best format). Higher priority over format.")
	sizeCmd.Flags().StringP("format", "f", "KB", "Format to print output. KB, MB, GB, TB")
}
