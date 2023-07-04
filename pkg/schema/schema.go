package schema

import (
	"fmt"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"os"
)

type Opts struct {
	format      string
	showTags    bool
	concurrency int64
}

func NewSchemaOpts(format string, showTags bool, concurrency int64) *Opts {
	opts := new(Opts)

	opts.format = format
	opts.showTags = showTags
	opts.concurrency = concurrency

	return opts
}

type Schema struct {
	file source.ParquetFile
	opts *Opts
}

func NewSchema(file source.ParquetFile, opts *Opts) *Schema {
	sc := new(Schema)

	sc.file = file
	sc.opts = opts

	return sc
}

func (sc *Schema) Show() {
	pr, err := reader.NewParquetReader(sc.file, nil, sc.opts.concurrency)

	if err != nil {
		fmt.Printf("error creating parquet reader %s\n", err)
		os.Exit(1)
	}

	tree := schematool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)

	if sc.opts.format == "go" {
		fmt.Printf("%s\n", tree.OutputStruct(sc.opts.showTags))
	} else {
		fmt.Printf("%s\n", tree.OutputJsonSchema())
	}
}