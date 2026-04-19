package main

import (
	"encoding/base64"
	"os"
)

const (
	fakeampsDataDirMode = 0o700
	fakeampsDataFileMode = 0o600
	fakeampsLogDirMode  = 0o750
)

func openFakeampsDiskRoot(path string) (*os.Root, error) {
	if err := os.MkdirAll(path, fakeampsDataDirMode); err != nil {
		return nil, err
	}
	return os.OpenRoot(path)
}

func encodeSOWTopicFilename(topic string) string {
	if topic == "" {
		return "topic.empty.sow"
	}
	return "topic." + base64.RawURLEncoding.EncodeToString([]byte(topic)) + ".sow"
}
