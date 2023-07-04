package schema_test

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"parquimetro/pkg/schema"
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

func TestSchema_Show(t *testing.T) {
	fakeParquetPath := fmt.Sprintf("/tmp/fake_parquet_test_%s.parquet", uuid.New().String())
	numberOfRows := 10

	GenerateFakeParquet(fakeParquetPath, numberOfRows)

	fr, err := local.NewLocalFileReader(fakeParquetPath)

	if err != nil {
		fmt.Printf("Error %s", err)
	}

	stdoutFileName := fmt.Sprintf("/tmp/output_%s.txt", uuid.New().String())
	file, err := os.Create(stdoutFileName)
	defer file.Close()
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}

	// Redirect the standard output to the file
	os.Stdout = file

	opts := schema.NewSchemaOpts("json", 1, true)
	schema.NewSchema(fr, opts).Show()

	outputFile, _ := os.ReadFile(stdoutFileName)

	subStr := "Tag\": \"name=Id, type=INT64, repetitiontype=REQUIRED"
	if !strings.Contains(string(outputFile), subStr) {
		t.Errorf("Parquet read doesn't contain expected output")
	}

	subStr = "Tag\": \"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"
	if !strings.Contains(string(outputFile), subStr) {
		t.Errorf("Parquet read doesn't contain expected output")
	}

	subStr = "Tag\": \"name=Age, type=INT32, repetitiontype=REQUIRED"
	if !strings.Contains(string(outputFile), subStr) {
		t.Errorf("Parquet read doesn't contain expected output")
	}

	os.Remove(stdoutFileName)
}
