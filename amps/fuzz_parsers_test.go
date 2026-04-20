package amps

import (
	"net/url"
	"testing"
)

func FuzzParseHeader(f *testing.F) {
	f.Add([]byte(`{"c":"publish","t":"orders","sub_id":"sub-1"}tail`))
	f.Add([]byte(`{"a":"processed,completed","c":"ack","status":"success"}`))
	f.Add([]byte(`{"bs":2,"c":"p"}`))
	f.Add([]byte(`not-json`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var message Message
		_, _ = parseHeader(&message, true, data)
	})
}

func FuzzParseBookmarkToken(f *testing.F) {
	f.Add("1|1|")
	f.Add("0")
	f.Add("1|150|3|,2|200|")
	f.Add("garbage")

	f.Fuzz(func(t *testing.T, bookmark string) {
		_, _, _ = parseBookmarkToken(bookmark)
	})
}

func FuzzParseSocketOptions(f *testing.F) {
	f.Add("compression=zlib")
	f.Add("tcp_nodelay=true&tcp_sndbuf=8192&tcp_rcvbuf=4096")
	f.Add("compression=snappy")
	f.Add("bad=%zz")

	f.Fuzz(func(t *testing.T, rawQuery string) {
		values, err := url.ParseQuery(rawQuery)
		if err != nil {
			return
		}
		_, _ = parseSocketOptions(values)
	})
}

func FuzzCompositeMessageParser(f *testing.F) {
	f.Add([]byte("4\\njson\\n5\\nhello"))
	f.Add([]byte(""))
	f.Add([]byte("broken"))

	f.Fuzz(func(t *testing.T, data []byte) {
		parser := NewCompositeMessageParser()
		size, _ := parser.Parse(data)
		for index := 0; index < size; index++ {
			_, _ = parser.Part(index)
		}
	})
}
