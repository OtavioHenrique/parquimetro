package reader

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"os"
)

type ReaderOpts struct {
	count       int64
	skip        int64
	concurrency int64
}

func NewReaderOpts(count int64, skip int64, concurrency int64) *ReaderOpts {
	opts := new(ReaderOpts)

	opts.count = count
	opts.skip = skip
	opts.concurrency = concurrency

	return opts
}

type Reader struct {
	file source.ParquetFile
	opts *ReaderOpts
}

func NewReader(file source.ParquetFile, opts *ReaderOpts) *Reader {
	pr := new(Reader)

	pr.file = file
	pr.opts = opts

	return pr
}

func (pr *Reader) Read() {
	reader, err := reader.NewParquetReader(pr.file, nil, pr.opts.concurrency)

	if err != nil {
		fmt.Printf("Can't create parquet reader %s\n", err)
		os.Exit(1)
	}

	totCnt := 0
	catCount := int(pr.opts.count)

	for totCnt < catCount {
		cnt := catCount - totCnt

		err = reader.SkipRows(0)
		if err != nil {
			fmt.Printf("Can't skip[: %s\n", err)
			os.Exit(1)
		}

		res, err := reader.ReadByNumber(cnt)
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
