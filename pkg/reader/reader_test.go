package reader_test

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/otaviohenrique/parquimetro/pkg/reader"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"os"
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

func TestReader_Read(t *testing.T) {
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

	opts := reader.NewReadOpts(2, 0)
	reader.NewReader(fr, 1).Read(opts)

	outputFile, _ := os.Open(stdoutFileName)
	defer outputFile.Close()
	scanner := bufio.NewScanner(outputFile)
	scanner.Scan()

	subStr := "[{\"Id\":0,\"Name\":\"PersonName0\",\"Age\":0},{\"Id\":1,\"Name\":\"PersonName1\",\"Age\":1}]"
	if !strings.Contains(scanner.Text(), subStr) {
		t.Errorf("Parquet read doesn't contain expected output")
	}

	os.Remove(stdoutFileName)
	os.Remove(fakeParquetPath)
}

func TestReader_ShowSchema(t *testing.T) {
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

	opts := reader.NewSchemaOpts("json", true)
	reader.NewReader(fr, 1).ShowSchema(opts)

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
	os.Remove(fakeParquetPath)
}

func TestSize_UncompressedSize(t *testing.T) {
	fakeParquetPath := fmt.Sprintf("/tmp/fake_parquet_test_%s.parquet", uuid.New().String())
	numberOfRows := 20

	GenerateFakeParquet(fakeParquetPath, numberOfRows)

	fr, err := local.NewLocalFileReader(fakeParquetPath)

	if err != nil {
		fmt.Printf("Error %s", err)
	}

	size := reader.NewReader(fr, 1)
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
