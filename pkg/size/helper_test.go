package size_test

import (
	"github.com/otaviohenrique/parquimetro/pkg/size"
	"testing"
)

func Test_formatBytes(t *testing.T) {
	type args struct {
		bytes float64
		unit  string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"Test invalid input", args{1024, "INVALID"}, "", true},
		{"Test KB input to KB", args{1024, "KB"}, "1.00 KB", false},
		{"Test KB input to KB", args{3000000, "KB"}, "2929.69 KB", false},
		{"Test 3MB input to MB", args{3000000, "MB"}, "2.86 MB", false},
		{"Test 0GB input to GB", args{3000000, "GB"}, "0.00 GB", false},
		{"Test 300MB input to GB", args{300000000, "GB"}, "0.28 GB", false},
		{"Test 1GB input to GB", args{1073741824, "GB"}, "1.00 GB", false},
		{"Test 1TB input to TB", args{1099511627776, "TB"}, "1.00 TB", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := size.FormatBytes(tt.args.bytes, tt.args.unit)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("formatBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_prettyFormatSize(t *testing.T) {
	type args struct {
		bytes int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"Test convert 1 KB", args{1000}, ""},
		{"Test convert KB", args{1024}, "1.00 KB"},
		{"Test convert 2 KB", args{2048}, "2.00 KB"},
		{"Test convert MB", args{1572864}, "1.50 MB"},
		{"Test convert MB", args{5368709120}, "5.00 GB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := size.PrettyFormatSize(tt.args.bytes); got != tt.want {
				t.Errorf("prettyFormatSize() = %v, want %v", got, tt.want)
			}
		})
	}
}
