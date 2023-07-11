package reader

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"os"
)

type SchemaOpts struct {
	format      string
	showTags    bool
	concurrency int64
}

func NewSchemaOpts(format string, showTags bool) *SchemaOpts {
	opts := new(SchemaOpts)

	opts.format = format
	opts.showTags = showTags

	return opts
}

type ReadOpts struct {
	count int64
	skip  int64
}

func NewReadOpts(count int64, skip int64) *ReadOpts {
	opts := new(ReadOpts)

	opts.count = count
	opts.skip = skip

	return opts
}

type Reader struct {
	file   source.ParquetFile
	reader *reader.ParquetReader
}

func NewReader(file source.ParquetFile, concurrency int64) *Reader {
	pr := new(Reader)

	pr.file = file

	reader, err := reader.NewParquetReader(pr.file, nil, concurrency)

	if err != nil {
		fmt.Printf("Can't create parquet reader %s\n", err)
		os.Exit(1)
	}

	pr.reader = reader

	return pr
}

func (pr *Reader) Read(opts *ReadOpts) {
	totCnt := 0
	catCount := int(opts.count)

	for totCnt < catCount {
		cnt := catCount - totCnt

		err := pr.reader.SkipRows(opts.skip)
		if err != nil {
			fmt.Printf("Can't skip[: %s\n", err)
			os.Exit(1)
		}

		res, err := pr.reader.ReadByNumber(cnt)
		if err != nil {
			fmt.Printf("Can't cat: %s\n", err)
			os.Exit(1)
		}

		jsonBs, err := json.Marshal(res)
		if err != nil {
			fmt.Printf("Can't to json: %s\n", err)
			os.Exit(1)
		}

		fmt.Println(string(jsonBs))

		totCnt += cnt
	}
}

func (pr *Reader) ShowSchema(opts *SchemaOpts) {
	tree := schematool.CreateSchemaTree(pr.reader.SchemaHandler.SchemaElements)

	if opts.format == "go" {
		fmt.Printf("%s\n", tree.OutputStruct(opts.showTags))
	} else {
		fmt.Printf("%s\n", tree.OutputJsonSchema())
	}
}

func (pr *Reader) UncompressedSize() int64 {
	var uncompressedSize int64
	for _, rg := range pr.reader.Footer.RowGroups {
		uncompressedSize += rg.TotalByteSize
	}

	return uncompressedSize
}

func (pr *Reader) CompressedSize() int64 {
	var compressedSize int64
	for _, rg := range pr.reader.Footer.RowGroups {
		for _, cc := range rg.Columns {
			compressedSize += cc.MetaData.TotalCompressedSize
		}
	}

	return compressedSize
}
