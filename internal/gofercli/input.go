package gofercli

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func parseDelimiter(raw string) (byte, error) {
	if raw == "" {
		return byte('\n'), nil
	}
	if len(raw) == 1 {
		return raw[0], nil
	}
	var value, err = strconv.Atoi(raw)
	if err != nil || value < 0 || value > 255 {
		return 0, fmt.Errorf("delimiter must be a single character or byte value")
	}
	return byte(value), nil
}

func splitPayloads(data []byte, delimiter byte) [][]byte {
	var rawParts = bytes.Split(data, []byte{delimiter})
	var payloads = make([][]byte, 0, len(rawParts))
	for _, part := range rawParts {
		if len(part) == 0 {
			continue
		}
		payloads = append(payloads, append([]byte(nil), part...))
	}
	return payloads
}

func readPayloadInputs(stdin io.Reader, inlineData string, filePath string, delimiter byte) ([][]byte, error) {
	if inlineData != "" {
		return [][]byte{[]byte(inlineData)}, nil
	}

	var data []byte
	var err error
	if filePath != "" {
		// #nosec G304 -- payload file path is an explicit CLI input.
		data, err = os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("read input file: %w", err)
		}
	} else {
		data, err = io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
	}

	if len(data) == 0 {
		return nil, nil
	}
	if isZIPArchive(data) {
		return readZIPPayloads(data)
	}
	return splitPayloads(data, delimiter), nil
}

func isZIPArchive(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == 'P' &&
		data[1] == 'K' &&
		data[2] == 0x03 &&
		data[3] == 0x04
}

func readZIPPayloads(data []byte) ([][]byte, error) {
	var reader, err = zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open zip archive: %w", err)
	}

	type zipEntry struct {
		name string
		data []byte
	}

	var entries []zipEntry
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}
		var handle, openErr = file.Open()
		if openErr != nil {
			return nil, fmt.Errorf("open zip entry %q: %w", file.Name, openErr)
		}
		var payload, readErr = io.ReadAll(handle)
		_ = handle.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read zip entry %q: %w", file.Name, readErr)
		}
		entries = append(entries, zipEntry{name: file.Name, data: payload})
	}

	sort.Slice(entries, func(left int, right int) bool {
		return naturalLess(entries[left].name, entries[right].name)
	})

	var payloads = make([][]byte, 0, len(entries))
	for _, entry := range entries {
		payloads = append(payloads, entry.data)
	}
	return payloads, nil
}

func naturalLess(left string, right string) bool {
	var leftName = path.Base(left)
	var rightName = path.Base(right)

	for leftName != "" && rightName != "" {
		var leftToken, leftDigits, leftRest = nextNaturalToken(leftName)
		var rightToken, rightDigits, rightRest = nextNaturalToken(rightName)

		if leftDigits && rightDigits {
			var leftNumber, leftErr = strconv.ParseUint(leftToken, 10, 64)
			var rightNumber, rightErr = strconv.ParseUint(rightToken, 10, 64)
			if leftErr == nil && rightErr == nil && leftNumber != rightNumber {
				return leftNumber < rightNumber
			}
		}

		if leftToken != rightToken {
			return leftToken < rightToken
		}

		leftName = leftRest
		rightName = rightRest
	}

	return leftName < rightName
}

func nextNaturalToken(value string) (string, bool, string) {
	if value == "" {
		return "", false, ""
	}

	var digitToken = unicode.IsDigit(rune(value[0]))
	var end int
	for end < len(value) {
		var isDigit = unicode.IsDigit(rune(value[end]))
		if isDigit != digitToken {
			break
		}
		end++
	}

	return value[:end], digitToken, value[end:]
}

func parseRateInterval(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	var value, err = strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parse rate: %w", err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("rate must be greater than zero")
	}
	var interval = time.Duration(float64(time.Second) / value)
	if interval <= 0 {
		return 0, fmt.Errorf("rate exceeds timer resolution")
	}
	return interval, nil
}
