package parquet_reader_test

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"parquimetro/pkg/parquet_reader"
	"strings"
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

func TestParquetReader_Read(t *testing.T) {
	fakeParquetPath := fmt.Sprintf("/tmp/fake_parquet_test_%s.parquet", uuid.New().String())
	numberOfRows := 10

	GenerateFakeParquet(fakeParquetPath, numberOfRows)

	fr, err := local.NewLocalFileReader(fakeParquetPath)

	if err != nil {
		fmt.Printf("Error %s", err)
	}

	file, err := os.Create("output.txt")
	defer file.Close()
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}

	// Redirect the standard output to the file
	os.Stdout = file

	opts := parquet_reader.NewParquetReaderOpts(2, 0, 1)
	parquet_reader.NewParquetReader(fr, opts).Read()

	file2, _ := os.Open("output.txt")
	defer file2.Close()
	scanner := bufio.NewScanner(file2)
	scanner.Scan()

	if !strings.Contains(scanner.Text(), "PersonName0") {
		t.Errorf("Parquet read doesn't contain expected output")
	}

	if !strings.Contains(scanner.Text(), "PersonName1") {
		t.Errorf("Parquet read doesn't contain expected output")
	}

	os.Remove("output.txt")
}
