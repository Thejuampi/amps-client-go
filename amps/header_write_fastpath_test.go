package amps

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

const strictWriteFixture = `{"c":"p","cid":"cmd-1","t":"orders","e":42,"filter":"/id > 10","opts":"replace","query_id":"qry-1","s":123456789,"sub_id":"sub-1"}`

func TestHeaderWriteStrictParityAdditionalFields(t *testing.T) {
	var zero uint
	var cases = []struct {
		name  string
		apply func(*_Header)
		field string
	}{
		{"group_sequence_number", func(h *_Header) { h.groupSequenceNumber = &zero }, `,"group_sequence_number":0`},
		{"skip_n", func(h *_Header) { h.skipN = &zero }, `,"skip_n":0`},
		{"maximum_messages", func(h *_Header) { h.maximumMessages = &zero }, `,"maximum_messages":0`},
		{"timeout_interval", func(h *_Header) { h.timeoutInterval = &zero }, `,"timeout_interval":0`},
		{"grace_period", func(h *_Header) { h.gracePeriod = &zero }, `,"grace_period":0`},
		{"data_only", func(h *_Header) { h.textExtras = &_HeaderTextExtras{dataOnly: []byte(`v"é`)} }, `,"data_only":"v\"é"`},
		{"send_empty", func(h *_Header) { h.textExtras = &_HeaderTextExtras{sendEmpty: []byte(`v"é`)} }, `,"send_empty":"v\"é"`},
		{"send_keys", func(h *_Header) { h.textExtras = &_HeaderTextExtras{sendKeys: []byte(`v"é`)} }, `,"send_keys":"v\"é"`},
		{"send_oof", func(h *_Header) { h.textExtras = &_HeaderTextExtras{sendOOF: []byte(`v"é`)} }, `,"send_oof":"v\"é"`},
		{"lease_period", func(h *_Header) { h.textExtras = &_HeaderTextExtras{leasePeriod: []byte(`v"é`)} }, `,"lease_period":"v\"é"`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var header = strictParityHeaderWriteBenchmarkHeader()
			testCase.apply(header)
			var buffer = bytes.NewBufferString("prefix:")
			if err := header.write(buffer); err != nil {
				t.Fatal(err)
			}
			var want = "prefix:" + strings.TrimSuffix(strictWriteFixture, "}") + testCase.field + "}"
			if buffer.String() != want {
				t.Fatalf("header output = %q; want %q", buffer.String(), want)
			}
		})
	}
}

func TestHeaderWriteStrictParityEmptyAdditionalFields(t *testing.T) {
	var cases = []struct {
		name  string
		apply func(*_HeaderTextExtras)
	}{
		{"data_only", func(e *_HeaderTextExtras) { e.dataOnly = []byte{} }},
		{"send_empty", func(e *_HeaderTextExtras) { e.sendEmpty = []byte{} }},
		{"send_keys", func(e *_HeaderTextExtras) { e.sendKeys = []byte{} }},
		{"send_oof", func(e *_HeaderTextExtras) { e.sendOOF = []byte{} }},
		{"lease_period", func(e *_HeaderTextExtras) { e.leasePeriod = []byte{} }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var header = strictParityHeaderWriteBenchmarkHeader()
			header.textExtras = &_HeaderTextExtras{}
			testCase.apply(header.textExtras)
			var buffer bytes.Buffer
			if err := header.write(&buffer); err != nil {
				t.Fatal(err)
			}
			var want = strings.TrimSuffix(strictWriteFixture, "}") + `,"` + testCase.name + `":""}`
			if buffer.String() != want {
				t.Fatalf("header output = %q; want %q", buffer.String(), want)
			}
		})
	}
	var header = strictParityHeaderWriteBenchmarkHeader()
	header.textExtras = &_HeaderTextExtras{}
	var buffer bytes.Buffer
	if !header.writeStrictParityFastPath(&buffer) || buffer.String() != strictWriteFixture {
		t.Fatalf("empty extras changed fast-path output: %q", buffer.String())
	}
}

func TestHeaderWriteStrictParityAllAdditionalFieldsInOrder(t *testing.T) {
	var zero uint
	var header = strictParityHeaderWriteBenchmarkHeader()
	header.groupSequenceNumber = &zero
	header.skipN = &zero
	header.maximumMessages = &zero
	header.timeoutInterval = &zero
	header.gracePeriod = &zero
	header.textExtras = &_HeaderTextExtras{dataOnly: []byte("d"), sendEmpty: []byte("e"), sendKeys: []byte("k"), sendOOF: []byte("o"), leasePeriod: []byte("l")}
	var buffer bytes.Buffer
	if err := header.write(&buffer); err != nil {
		t.Fatal(err)
	}
	var want = strings.TrimSuffix(strictWriteFixture, "}") + `,"data_only":"d","send_empty":"e","send_keys":"k","send_oof":"o","lease_period":"l","group_sequence_number":0,"skip_n":0,"maximum_messages":0,"timeout_interval":0,"grace_period":0}`
	if buffer.String() != want {
		t.Fatalf("header output = %q; want %q", buffer.String(), want)
	}
}

func TestHeaderBufferHasRoomBoundaries(t *testing.T) {
	var buffer = bytes.NewBuffer(make([]byte, 0, 20))
	var cases = []struct {
		name   string
		fixed  int
		fields [][]byte
		want   bool
	}{
		{"fixed_exceeds_capacity", 21, nil, false},
		{"fixed_exact", 20, nil, true},
		{"empty_field_exact", 20, [][]byte{{}}, true},
		{"first_field_exceeds_capacity", 20, [][]byte{{1}}, false},
		{"fields_exact", 2, [][]byte{make([]byte, 3), make([]byte, 15)}, true},
		{"last_field_one_short", 2, [][]byte{make([]byte, 3), make([]byte, 16)}, false},
		{"large_fixed", int(^uint(0) >> 1), nil, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := headerBufferHasRoom(buffer, testCase.fixed, testCase.fields...); got != testCase.want {
				t.Fatalf("fits = %v; want %v", got, testCase.want)
			}
		})
	}
}

func TestHeaderWriteStrictParityStringFields(t *testing.T) {
	var value = "\"\\\x00\x01\x02\x03\x04\x05\x06\x07\b\t\n\v\f\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1fΩ\u2028\u2029"
	var quoted = `"\"\\\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000b\f\r\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001fΩ` + "\u2028\u2029" + `"`
	var cases = []struct {
		name  string
		old   string
		apply func(*_Header, []byte)
	}{
		{"cid", `"cid":"cmd-1"`, func(h *_Header, v []byte) { h.commandID = v }},
		{"t", `"t":"orders"`, func(h *_Header, v []byte) { h.topic = v }},
		{"filter", `"filter":"/id > 10"`, func(h *_Header, v []byte) { h.filter = v }},
		{"opts", `"opts":"replace"`, func(h *_Header, v []byte) { h.options = v }},
		{"query_id", `"query_id":"qry-1"`, func(h *_Header, v []byte) { h.queryID = v }},
		{"sub_id", `"sub_id":"sub-1"`, func(h *_Header, v []byte) { h.subID = v }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var header = strictParityHeaderWriteBenchmarkHeader()
			testCase.apply(header, []byte(value))
			var buffer bytes.Buffer
			if err := header.write(&buffer); err != nil {
				t.Fatal(err)
			}
			var want = strings.Replace(strictWriteFixture, testCase.old, `"`+testCase.name+`":`+quoted, 1)
			if buffer.String() != want {
				t.Fatalf("header output = %q; want %q", buffer.String(), want)
			}
		})
	}
}

func TestHeaderWriteStrictParityNilAndEmptyStrings(t *testing.T) {
	var cases = []struct {
		name  string
		old   string
		apply func(*_Header, []byte)
	}{
		{"cid", `,"cid":"cmd-1"`, func(h *_Header, v []byte) { h.commandID = v }},
		{"t", `,"t":"orders"`, func(h *_Header, v []byte) { h.topic = v }},
		{"filter", `,"filter":"/id > 10"`, func(h *_Header, v []byte) { h.filter = v }},
		{"opts", `,"opts":"replace"`, func(h *_Header, v []byte) { h.options = v }},
		{"query_id", `,"query_id":"qry-1"`, func(h *_Header, v []byte) { h.queryID = v }},
		{"sub_id", `,"sub_id":"sub-1"`, func(h *_Header, v []byte) { h.subID = v }},
	}
	for _, testCase := range cases {
		for _, present := range []bool{false, true} {
			var name = testCase.name + "/nil"
			var replacement string
			var value []byte
			if present {
				name = testCase.name + "/empty"
				value = []byte{}
				replacement = `,"` + testCase.name + `":""`
			}
			t.Run(name, func(t *testing.T) {
				var header = strictParityHeaderWriteBenchmarkHeader()
				testCase.apply(header, value)
				var buffer bytes.Buffer
				if err := header.write(&buffer); err != nil {
					t.Fatal(err)
				}
				var want = strings.Replace(strictWriteFixture, testCase.old, replacement, 1)
				if buffer.String() != want {
					t.Fatalf("header output = %q; want %q", buffer.String(), want)
				}
			})
		}
	}
}

func TestHeaderWriteStrictParityBufferAndNumericChanges(t *testing.T) {
	for _, capacity := range []int{0, 8, len("discard-prefix:") + len(strictWriteFixture) - 1, len("discard-prefix:") + len(strictWriteFixture), 256} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			var header = strictParityHeaderWriteBenchmarkHeader()
			var buffer = bytes.NewBuffer(make([]byte, 0, capacity))
			_, _ = buffer.WriteString("discard-prefix:")
			_ = buffer.Next(len("discard-"))
			if err := header.write(buffer); err != nil {
				t.Fatal(err)
			}
			if buffer.String() != "prefix:"+strictWriteFixture {
				t.Fatalf("prefixed header output = %q", buffer.String())
			}
			*header.expiration = ^uint(0)
			*header.sequenceID = ^uint64(0)
			buffer.Reset()
			_, _ = buffer.WriteString("prefix:")
			if err := header.write(buffer); err != nil {
				t.Fatal(err)
			}
			var want = strings.Replace(strictWriteFixture, `"e":42`, `"e":`+strconv.FormatUint(uint64(^uint(0)), 10), 1)
			want = strings.Replace(want, `"s":123456789`, `"s":18446744073709551615`, 1)
			if buffer.String() != "prefix:"+want {
				t.Fatalf("maximum numeric output = %q; want %q", buffer.String(), "prefix:"+want)
			}
			*header.expiration = 0
			*header.sequenceID = 0
			buffer.Reset()
			if err := header.write(buffer); err != nil {
				t.Fatal(err)
			}
			want = strings.Replace(strictWriteFixture, `"e":42`, `"e":0`, 1)
			want = strings.Replace(want, `"s":123456789`, `"s":0`, 1)
			if buffer.String() != want {
				t.Fatalf("zero numeric output = %q; want %q", buffer.String(), want)
			}
		})
	}
}

func TestHeaderWriteStrictParityCacheInvalidatedBySettersAndReset(t *testing.T) {
	var command = &Command{header: strictParityHeaderWriteBenchmarkHeader()}
	var buffer bytes.Buffer
	if err := command.header.write(&buffer); err != nil {
		t.Fatal(err)
	}
	command.SetTopic("q\"\\\nΩ")
	command.SetFilter("/id\x00")
	command.SetExpiration(7)
	command.SetSequenceID(^uint64(0))
	buffer.Reset()
	if err := command.header.write(&buffer); err != nil {
		t.Fatal(err)
	}
	var want = `{"c":"p","cid":"cmd-1","t":"q\"\\\nΩ","e":7,"filter":"/id\u0000","opts":"replace","query_id":"qry-1","s":18446744073709551615,"sub_id":"sub-1"}`
	if buffer.String() != want {
		t.Fatalf("updated header output = %q; want %q", buffer.String(), want)
	}
	command.header.reset()
	command.header.command = CommandPublish
	command.SetCommandID("new").SetTopic("fresh").SetExpiration(1).SetFilter("f").SetOptions("o").SetQueryID("q").SetSequenceID(2).SetSubID("s")
	buffer.Reset()
	if err := command.header.write(&buffer); err != nil {
		t.Fatal(err)
	}
	want = `{"c":"p","cid":"new","t":"fresh","e":1,"filter":"f","opts":"o","query_id":"q","s":2,"sub_id":"s"}`
	if buffer.String() != want {
		t.Fatalf("reset header output = %q; want %q", buffer.String(), want)
	}
}

func TestHeaderWriteStrictParityPreallocatedBufferAllocations(t *testing.T) {
	var header = strictParityHeaderWriteBenchmarkHeader()
	var buffer = bytes.NewBuffer(make([]byte, 0, 256))
	var allocations = testing.AllocsPerRun(1000, func() {
		buffer.Reset()
		if err := header.write(buffer); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 || buffer.String() != strictWriteFixture {
		t.Fatalf("allocations = %v, output = %q", allocations, buffer.String())
	}
}

func TestHeaderWriteSimplePublishBuffersAndEscapes(t *testing.T) {
	var cases = []struct {
		name    string
		command string
		topic   string
		want    string
	}{
		{"ascii", "cid-1", "orders", `{"c":"p","cid":"cid-1","t":"orders"}`},
		{"empty", "", "", `{"c":"p","cid":"","t":""}`},
		{"escaped", "c\"\\\x00", "Ω\n\x1f", `{"c":"p","cid":"c\"\\\u0000","t":"Ω\n\u001f"}`},
	}
	for _, testCase := range cases {
		for _, capacity := range []int{0, 8, len("discard-prefix:") + len(testCase.want) - 1, len("discard-prefix:") + len(testCase.want), 256} {
			t.Run(testCase.name+"/"+strconv.Itoa(capacity), func(t *testing.T) {
				var header = &_Header{command: CommandPublish, commandID: []byte(testCase.command), topic: []byte(testCase.topic)}
				var buffer = bytes.NewBuffer(make([]byte, 0, capacity))
				_, _ = buffer.WriteString("discard-prefix:")
				_ = buffer.Next(len("discard-"))
				if err := header.write(buffer); err != nil {
					t.Fatal(err)
				}
				if buffer.String() != "prefix:"+testCase.want {
					t.Fatalf("header output = %q; want %q", buffer.String(), "prefix:"+testCase.want)
				}
			})
		}
	}
}

func TestHeaderWriteSimplePublishCacheAndAllocations(t *testing.T) {
	var command = NewCommand("publish").SetCommandID("cid-1").SetTopic("orders")
	var buffer = bytes.NewBuffer(make([]byte, 0, 256))
	var allocations = testing.AllocsPerRun(1000, func() {
		buffer.Reset()
		if err := command.header.write(buffer); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 || buffer.String() != `{"c":"p","cid":"cid-1","t":"orders"}` {
		t.Fatalf("allocations = %v, output = %q", allocations, buffer.String())
	}
	command.SetTopic("escaped\"\\\nΩ")
	buffer.Reset()
	if err := command.header.write(buffer); err != nil {
		t.Fatal(err)
	}
	if buffer.String() != `{"c":"p","cid":"cid-1","t":"escaped\"\\\nΩ"}` {
		t.Fatalf("updated header output = %q", buffer.String())
	}
}

func TestHeaderWriteSimplePublishNilStrings(t *testing.T) {
	var cases = []struct {
		name      string
		commandID []byte
		topic     []byte
		want      string
	}{
		{"nil_cid", nil, []byte("orders"), `{"c":"p","t":"orders"}`},
		{"nil_topic", []byte("cid-1"), nil, `{"c":"p","cid":"cid-1"}`},
		{"both_nil", nil, nil, `{"c":"p"}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var header = &_Header{command: CommandPublish, commandID: testCase.commandID, topic: testCase.topic}
			var buffer bytes.Buffer
			if err := header.write(&buffer); err != nil {
				t.Fatal(err)
			}
			if buffer.String() != testCase.want {
				t.Fatalf("header output = %q; want %q", buffer.String(), testCase.want)
			}
		})
	}
}

func TestHeaderWriteGrowthAllocationBudget(t *testing.T) {
	var strict = strictParityHeaderWriteBenchmarkHeader()
	var simple = &_Header{command: CommandPublish, commandID: []byte("cid-1"), topic: []byte("orders")}
	var cases = []struct {
		name     string
		header   *_Header
		capacity int
		strict   bool
	}{
		{"strict/empty", strict, 0, true},
		{"strict/one_short", strict, len("prefix:") + len(strictWriteFixture) - 1, true},
		{"simple/empty", simple, 0, false},
		{"simple/one_short", simple, len("prefix:") + len(`{"c":"p","cid":"cid-1","t":"orders"}`) - 1, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var before = testing.AllocsPerRun(100, func() {
				var buffer = bytes.NewBuffer(make([]byte, 0, testCase.capacity))
				_, _ = buffer.WriteString("prefix:")
				if testCase.strict {
					writeStrictParityBeforeFusion(testCase.header, buffer)
				} else {
					writeSimplePublishBeforeFusion(testCase.header, buffer)
				}
			})
			var allocations = testing.AllocsPerRun(100, func() {
				var buffer = bytes.NewBuffer(make([]byte, 0, testCase.capacity))
				_, _ = buffer.WriteString("prefix:")
				if err := testCase.header.write(buffer); err != nil {
					t.Fatal(err)
				}
			})
			if allocations > before {
				t.Fatalf("allocations = %v; before fusion %v", allocations, before)
			}
		})
	}
}

// These are the pre-fusion writes, retained only as allocation baselines.
func writeStrictParityBeforeFusion(header *_Header, buffer *bytes.Buffer) {
	_, _ = buffer.WriteString(`{"c":"p","cid":"`)
	_, _ = buffer.Write(header.commandID)
	_, _ = buffer.WriteString(`","t":"`)
	_, _ = buffer.Write(header.topic)
	_, _ = buffer.WriteString(`","e":`)
	_ = writeUintToBuffer(buffer, uint64(*header.expiration))
	_, _ = buffer.WriteString(`,"filter":"`)
	_, _ = buffer.Write(header.filter)
	_, _ = buffer.WriteString(`","opts":"`)
	_, _ = buffer.Write(header.options)
	_, _ = buffer.WriteString(`","query_id":"`)
	_, _ = buffer.Write(header.queryID)
	_, _ = buffer.WriteString(`","s":`)
	_ = writeUintToBuffer(buffer, *header.sequenceID)
	_, _ = buffer.WriteString(`,"sub_id":"`)
	_, _ = buffer.Write(header.subID)
	_, _ = buffer.WriteString(`"}`)
}

func writeSimplePublishBeforeFusion(header *_Header, buffer *bytes.Buffer) {
	_, _ = buffer.WriteString(`{"c":"p","cid":"`)
	_, _ = buffer.Write(header.commandID)
	_, _ = buffer.WriteString(`","t":"`)
	_, _ = buffer.Write(header.topic)
	_, _ = buffer.WriteString(`"}`)
}
