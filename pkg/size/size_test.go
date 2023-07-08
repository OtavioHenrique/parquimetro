package size_test

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/otaviohenrique/parquimetro/pkg/size"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"testing"
)

type TestPerson struct {
	Id   int    `parquet:"name=id, type=INT64"`
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age  int    `parquet:"name=age, type=INT32"`
}

func GenerateFakeParquet(path string, rows int) error {
	fw, err := local.NewLocalFileWriter(path)

	if err != nil {
		return err
	}

	pw, err := writer.NewParquetWriter(fw, new(TestPerson), 1)

	if err != nil {
		return err
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := 0; i < rows; i++ {
		person := TestPerson{
			Id:   i,
			Name: fmt.Sprintf("PersonName%d", i),
			Age:  i,
		}

		if err = pw.Write(person); err != nil {
			return err
		}
	}

	if err = pw.WriteStop(); err != nil {
		return err
	}

	fw.Close()

	return nil
}

func TestSize_UncompressedSize(t *testing.T) {
	fakeParquetPath := fmt.Sprintf("/tmp/fake_parquet_test_%s.parquet", uuid.New().String())
	numberOfRows := 20

	GenerateFakeParquet(fakeParquetPath, numberOfRows)

	fr, err := local.NewLocalFileReader(fakeParquetPath)

	if err != nil {
		fmt.Printf("Error %s", err)
	}

	opts := size.NewOpts(1)
	size := size.NewSize(fr, opts)
	var expectedUnSize int64 = 851

	if uncompressedSize := size.UncompressedSize(); uncompressedSize != expectedUnSize {
		t.Errorf("Wrong Uncompressed size. Expected %d, got %d", expectedUnSize, uncompressedSize)
	}

	var expectedComSize int64 = 575
	if compressedSize := size.CompressedSize(); compressedSize != expectedComSize {
		t.Errorf("Wrong Compressed size. Expected %d, got %d", expectedComSize, compressedSize)
	}

	os.Remove(fakeParquetPath)
}
