package size

import (
	"fmt"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"os"
)

type Opts struct {
	concurrency int64
}

func NewOpts(concurrency int64) *Opts {
	opts := new(Opts)

	opts.concurrency = concurrency

	return opts
}

type Size struct {
	file source.ParquetFile
	opts *Opts
}

func NewSize(file source.ParquetFile, opts *Opts) *Size {
	size := new(Size)

	size.file = file
	size.opts = opts

	return size
}

func (size *Size) UncompressedSize() int64 {
	pr, err := reader.NewParquetReader(size.file, nil, size.opts.concurrency)

	if err != nil {
		fmt.Printf("error creating parquet reader %s\n", err)
		os.Exit(1)
	}

	var uncompressedSize int64
	for _, rg := range pr.Footer.RowGroups {
		uncompressedSize += rg.TotalByteSize
	}

	return uncompressedSize
}

func (size *Size) CompressedSize() int64 {
	pr, err := reader.NewParquetReader(size.file, nil, size.opts.concurrency)

	if err != nil {
		fmt.Printf("error creating parquet reader %s\n", err)
		os.Exit(1)
	}

	var compressedSize int64
	for _, rg := range pr.Footer.RowGroups {
		for _, cc := range rg.Columns {
			compressedSize += cc.MetaData.TotalCompressedSize
		}
	}

	return compressedSize
}
