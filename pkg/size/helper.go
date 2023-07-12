package size

import (
	"errors"
	"fmt"
)

func FormatBytes(bytes float64, unit string) (string, error) {
	var size float64

	switch unit {
	case "MB":
		size = bytes / (1024 * 1024)
	case "KB":
		size = bytes / 1024
	case "GB":
		size = bytes / (1024 * 1024 * 1024)
	case "TB":
		size = bytes / (1024 * 1024 * 1024 * 1024)
	default:
		return "", errors.New("invalid input given")
	}

	return fmt.Sprintf("%.2f %s", size, unit), nil
}

func PrettyFormatSize(bytes int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	base := int64(1024)

	if bytes < base {
		fmt.Printf("%d %s", bytes, units[0])
		return ""
	}

	i := 1
	size := float64(bytes)
	for size >= float64(base) && i < len(units) {
		size /= float64(base)
		i++
	}

	return fmt.Sprintf("%.2f %s", size, units[i-1])
}
