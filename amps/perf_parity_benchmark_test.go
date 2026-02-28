package amps

import "testing"

var strictParityHeaderFrame = []byte(`{"c":"p","t":"orders","sub_id":"sub-1"}{"id":1}`)

func BenchmarkStrictParityHeaderHotParse(b *testing.B) {
	var msg = &Message{header: new(_Header)}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		var _, err = parseHeader(msg, true, strictParityHeaderFrame)
		if err != nil {
			b.Fatalf("parse header failed: %v", err)
		}
	}
}

func BenchmarkStrictParitySOWBatchParse(b *testing.B) {
	var msg = &Message{header: new(_Header)}

	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		for inner := 0; inner < 5; inner++ {
			var _, err = parseHeader(msg, true, strictParityHeaderFrame)
			if err != nil {
				b.Fatalf("parse header failed: %v", err)
			}
		}
	}
}
