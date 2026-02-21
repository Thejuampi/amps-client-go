package amps

import (
	"bytes"
	"math"
	"sync"
	"testing"
)

func TestCompositeMessageBuilderAndParserCoverage(t *testing.T) {
	builder := NewCompositeMessageBuilder()
	if builder == nil {
		t.Fatalf("expected composite builder")
	}

	if err := builder.Append("alpha"); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := builder.AppendBytes([]byte("xyz"), 1, 2); err != nil {
		t.Fatalf("append bytes failed: %v", err)
	}
	if err := builder.AppendBytes([]byte("xyz"), 1, 3); err == nil {
		t.Fatalf("expected append bytes range validation error")
	}
	if err := builder.AppendBytes([]byte("ignore"), 0, 0); err != nil {
		t.Fatalf("append zero length failed: %v", err)
	}

	raw := builder.GetBytes()
	if len(raw) == 0 || builder.GetData() == "" {
		t.Fatalf("expected serialized composite payload")
	}

	parser := NewCompositeMessageParser()
	if parser == nil {
		t.Fatalf("expected parser")
	}
	count, err := parser.Parse(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if count != 2 || parser.Size() != 2 {
		t.Fatalf("expected 2 parts, got %d", parser.Size())
	}

	part0, err := parser.Part(0)
	if err != nil || string(part0) != "alpha" {
		t.Fatalf("unexpected part0: %q err=%v", string(part0), err)
	}
	part1, err := parser.Part(1)
	if err != nil || len(part1) != 2 || part1[0] != 'y' || part1[1] != 'z' {
		t.Fatalf("unexpected part1 payload: %v err=%v", part1, err)
	}
	if _, err := parser.Part(-1); err == nil {
		t.Fatalf("expected invalid negative index error")
	}
	if _, err := parser.Part(3); err == nil {
		t.Fatalf("expected invalid high index error")
	}

	message := &Message{header: new(_Header)}
	message.SetData(raw)
	if parsed, err := parser.ParseMessage(message); err != nil || parsed != 2 {
		t.Fatalf("parse message failed: parsed=%d err=%v", parsed, err)
	}

	// Invalid part length branch.
	_, err = parser.Parse([]byte{0, 0, 0, 6, 'a'})
	if err == nil {
		t.Fatalf("expected invalid part length error")
	}
	// High-bit set length should be rejected by unsigned bounds check.
	_, err = parser.Parse([]byte{0x80, 0x00, 0x00, 0x00})
	if err == nil {
		t.Fatalf("expected high-bit length validation error")
	}

	builder.Clear()
	if len(builder.GetBytes()) != 0 {
		t.Fatalf("expected clear to reset builder")
	}
}

func TestCompositeMessageBuilderAndParserErrorCoverage(t *testing.T) {
	builder := NewCompositeMessageBuilder()
	if err := builder.AppendBytes([]byte("abc"), 0, -1); err == nil {
		t.Fatalf("expected negative length error")
	}
	if err := builder.AppendBytes([]byte("abc"), -1, 1); err == nil {
		t.Fatalf("expected negative offset error")
	}
	if err := builder.AppendBytes(nil, 0, 1); err == nil {
		t.Fatalf("expected nil data error")
	}
	if err := builder.AppendBytes([]byte("abc"), 4, 1); err == nil {
		t.Fatalf("expected out-of-range offset error")
	}
	if err := builder.AppendBytes([]byte("abc"), 2, 2); err == nil {
		t.Fatalf("expected out-of-range end error")
	}
	if err := builder.AppendBytes([]byte("abc"), 1, math.MaxInt); err == nil {
		t.Fatalf("expected overflow-safe range error")
	}

	parser := NewCompositeMessageParser()
	if parsed, err := parser.Parse(nil); err != nil || parsed != 0 {
		t.Fatalf("expected empty parse success, got parsed=%d err=%v", parsed, err)
	}
	if _, err := parser.Parse([]byte{0, 0, 0}); err == nil {
		t.Fatalf("expected truncated header error")
	}
	if _, err := parser.ParseMessage(nil); err == nil {
		t.Fatalf("expected nil message parse error")
	}
}

func TestFixAndNVFixBuildersCoverage(t *testing.T) {
	fixBuilder := NewFIXBuilder()
	if fixBuilder == nil {
		t.Fatalf("expected fix builder")
	}
	if digits := fixBuilder.checkIfLog10(9); digits != 1 {
		t.Fatalf("unexpected log10 digits for 9: %d", digits)
	}
	if digits := fixBuilder.checkIfLog10(10); digits != 2 {
		t.Fatalf("unexpected log10 digits for 10: %d", digits)
	}
	if digits := fixBuilder.checkIfLog10(-1); digits != 0 {
		t.Fatalf("unexpected log10 digits for negative tag: %d", digits)
	}

	fixBuilder.checkCapacity(4096)
	if fixBuilder.capacity < 4096 {
		t.Fatalf("expected capacity growth")
	}

	if err := fixBuilder.AppendBytes(-1, []byte("bad"), 0, 3); err == nil {
		t.Fatalf("expected negative tag error")
	}
	if err := fixBuilder.AppendBytes(35, []byte("A"), 0, 1); err != nil {
		t.Fatalf("append bytes failed: %v", err)
	}
	if err := fixBuilder.Append(49, "BUYER"); err != nil {
		t.Fatalf("append string failed: %v", err)
	}
	if fixBuilder.Size() == 0 || len(fixBuilder.Bytes()) == 0 || fixBuilder.Data() == "" {
		t.Fatalf("expected serialized FIX payload")
	}
	fixBuilder.Clear()
	if fixBuilder.Size() != 0 || len(fixBuilder.Bytes()) != 0 {
		t.Fatalf("expected clear to reset fix builder")
	}

	customFixBuilder := NewFIXBuilder('|')
	if err := customFixBuilder.Append(8, "FIX.4.4"); err != nil {
		t.Fatalf("append to custom fix builder failed: %v", err)
	}
	customShredder := NewFIXShredder('|')
	fixMap := customShredder.ToMap(customFixBuilder.Bytes())
	if fixMap[8] != "FIX.4.4" {
		t.Fatalf("unexpected FIX map value: %+v", fixMap)
	}

	nvBuilder := NewNVFIXBuilder()
	if nvBuilder == nil {
		t.Fatalf("expected nvfix builder")
	}
	nvBuilder.checkCapacity(4096)
	if nvBuilder.capacity < 4096 {
		t.Fatalf("expected nvfix capacity growth")
	}
	if err := nvBuilder.AppendBytes(nil, []byte("value"), 0, 5); err == nil {
		t.Fatalf("expected empty tag error")
	}
	if err := nvBuilder.AppendBytes([]byte("symbol"), []byte("AAPL"), 0, 4); err != nil {
		t.Fatalf("append bytes failed: %v", err)
	}
	if err := nvBuilder.AppendStrings("venue", "XNAS"); err != nil {
		t.Fatalf("append strings failed: %v", err)
	}
	if nvBuilder.Size() == 0 || len(nvBuilder.Bytes()) == 0 || nvBuilder.Data() == "" {
		t.Fatalf("expected serialized NVFIX payload")
	}

	nvShredder := NewNVFIXShredder()
	nvMap := nvShredder.ToMap(nvBuilder.Bytes())
	if nvMap["symbol"] != "AAPL" || nvMap["venue"] != "XNAS" {
		t.Fatalf("unexpected NVFIX map: %+v", nvMap)
	}

	nvBuilder.Clear()
	if nvBuilder.Size() != 0 || len(nvBuilder.Bytes()) != 0 {
		t.Fatalf("expected clear to reset nvfix builder")
	}

	customNVBuilder := NewNVFIXBuilder('|')
	if err := customNVBuilder.AppendStrings("k", "v"); err != nil {
		t.Fatalf("append custom nvfix failed: %v", err)
	}
	customNVShredder := NewNVFIXShredder('|')
	customNVMap := customNVShredder.ToMap(customNVBuilder.Bytes())
	if customNVMap["k"] != "v" {
		t.Fatalf("unexpected custom NVFIX map: %+v", customNVMap)
	}
}

func TestFixAndNVFixBuildersRangeErrorsCoverage(t *testing.T) {
	fixBuilder := NewFIXBuilder()
	if err := fixBuilder.AppendBytes(35, []byte("A"), -1, 1); err == nil {
		t.Fatalf("expected negative offset range error")
	}
	if err := fixBuilder.AppendBytes(35, []byte("A"), 0, -1); err == nil {
		t.Fatalf("expected negative length range error")
	}
	if err := fixBuilder.AppendBytes(35, []byte("A"), 2, 1); err == nil {
		t.Fatalf("expected offset out-of-range error")
	}
	if err := fixBuilder.AppendBytes(35, []byte("A"), 0, 2); err == nil {
		t.Fatalf("expected offset+length out-of-range error")
	}

	nvBuilder := NewNVFIXBuilder()
	if err := nvBuilder.AppendBytes([]byte("k"), []byte("v"), -1, 1); err == nil {
		t.Fatalf("expected negative value offset error")
	}
	if err := nvBuilder.AppendBytes([]byte("k"), []byte("v"), 0, -1); err == nil {
		t.Fatalf("expected negative value length error")
	}
	if err := nvBuilder.AppendBytes([]byte("k"), []byte("v"), 2, 1); err == nil {
		t.Fatalf("expected value offset out-of-range error")
	}
	if err := nvBuilder.AppendBytes([]byte("k"), []byte("v"), 0, 2); err == nil {
		t.Fatalf("expected value offset+length out-of-range error")
	}
}

func TestNVFIXBuilderGrowthPreservesExistingBytes(t *testing.T) {
	builder := NewNVFIXBuilder()
	if builder == nil {
		t.Fatalf("expected NVFIX builder")
	}

	// Force a tiny capacity so the second append must trigger reallocation.
	builder.capacity = 8
	if err := builder.AppendStrings("a", "1"); err != nil {
		t.Fatalf("first append failed: %v", err)
	}
	before := append([]byte(nil), builder.Bytes()...)

	large := make([]byte, 64)
	for idx := range large {
		large[idx] = 'x'
	}
	if err := builder.AppendBytes([]byte("b"), large, 0, len(large)); err != nil {
		t.Fatalf("second append failed: %v", err)
	}

	after := builder.Bytes()
	if len(after) <= len(before) {
		t.Fatalf("expected grown payload, before=%d after=%d", len(before), len(after))
	}
	if string(after[:len(before)]) != string(before) {
		t.Fatalf("expected existing payload bytes preserved across growth")
	}
}

func TestFIXBuilderGrowthPreservesExistingBytes(t *testing.T) {
	builder := NewFIXBuilder()
	if builder == nil {
		t.Fatalf("expected FIX builder")
	}

	// Force a tiny capacity so the second append must trigger reallocation.
	builder.capacity = 8
	if err := builder.Append(8, "FIX.4.4"); err != nil {
		t.Fatalf("first append failed: %v", err)
	}
	before := append([]byte(nil), builder.Bytes()...)

	large := make([]byte, 64)
	for idx := range large {
		large[idx] = 'y'
	}
	if err := builder.AppendBytes(35, large, 0, len(large)); err != nil {
		t.Fatalf("second append failed: %v", err)
	}

	after := builder.Bytes()
	if len(after) <= len(before) {
		t.Fatalf("expected grown payload, before=%d after=%d", len(before), len(after))
	}
	if string(after[:len(before)]) != string(before) {
		t.Fatalf("expected existing payload bytes preserved across growth")
	}
}

func TestMessageRouterCoverage(t *testing.T) {
	router := &MessageRouter{
		routes: new(sync.Map),
		key:    "route-1",
	}

	delivered := 0
	handler := func(message *Message) error {
		delivered++
		return nil
	}
	router.AddRoute("route-1", handler, AckTypeReceived|AckTypeProcessed, AckTypeNone, false, false)

	route := router.FindRoute("route-1")
	if route == nil {
		t.Fatalf("expected route lookup")
	}
	if err := route(&Message{header: new(_Header)}); err != nil {
		t.Fatalf("route callback failed: %v", err)
	}
	if delivered == 0 {
		t.Fatalf("expected route callback invocation")
	}

	dataMessage := &Message{
		header: &_Header{
			commandID: []byte("route-1"),
			queryID:   []byte("route-1"),
			subID:     []byte("route-1"),
		},
	}
	if got := router.DeliverData(dataMessage); got == 0 {
		t.Fatalf("expected data delivery")
	}
	if got := router.DeliverDataWithID(dataMessage, "missing"); got != 0 {
		t.Fatalf("expected no delivery for missing route, got %d", got)
	}

	ackReceived := AckTypeReceived
	ackMessage := &Message{
		header: &_Header{
			command:   CommandAck,
			commandID: []byte("route-1"),
			queryID:   []byte("route-1"),
			subID:     []byte("route-1"),
			ackType:   &ackReceived,
		},
	}
	if got := router.DeliverAck(ackMessage, AckTypeReceived); got == 0 {
		t.Fatalf("expected ack delivery")
	}

	ackProcessed := AckTypeProcessed
	ackTerminate := &Message{
		header: &_Header{
			command:   CommandAck,
			commandID: []byte("route-1"),
			ackType:   &ackProcessed,
		},
	}
	if got := router.DeliverAck(ackTerminate, AckTypeProcessed); got == 0 {
		t.Fatalf("expected processed ack delivery")
	}
	if _, exists := router.routes.Load("route-1"); exists {
		t.Fatalf("expected route removal on termination ack")
	}

	router.AddRoute("route-2", func(*Message) error { return nil }, AckTypeNone, AckTypeNone, true, false)
	router.RemoveRoute("route-2")
	if _, exists := router.routes.Load("route-2"); exists {
		t.Fatalf("expected explicit remove route")
	}

	router.AddRoute("a", func(*Message) error { return nil }, AckTypeNone, AckTypeNone, true, false)
	router.AddRoute("b", func(*Message) error { return nil }, AckTypeNone, AckTypeNone, true, false)
	router.UnsubscribeAll()
	remaining := 0
	router.routes.Range(func(_, _ any) bool {
		remaining++
		return true
	})
	if remaining != 0 {
		t.Fatalf("expected unsubscribe all to clear routes")
	}

	router.Clear()
	if router.routes == nil {
		t.Fatalf("clear should keep a map instance")
	}
	if route := router.FindRoute("missing"); route != nil {
		t.Fatalf("expected missing route lookup to return nil")
	}

	router.MessageRoute.messageHandler = nil
	router.routes.Store("route-nil", func(*Message) error { return nil })
	router.processAckForRemoval(AckTypeProcessed, "route-nil")
	router.processAckForRemoval(AckTypeProcessed, "missing")

	router = &MessageRouter{routes: new(sync.Map), key: "route-q"}
	router.AddRoute("route-q", func(*Message) error { return nil }, AckTypeProcessed, AckTypeNone, false, false)
	ackProcessed = AckTypeProcessed
	if delivered := router.DeliverAck(&Message{header: &_Header{
		command:   CommandAck,
		commandID: []byte("route-q"),
		queryID:   []byte("route-q"),
		ackType:   &ackProcessed,
		status:    []byte("success"),
	}}, AckTypeProcessed); delivered == 0 {
		t.Fatalf("expected query-id ack delivery")
	}

	router = &MessageRouter{routes: new(sync.Map), key: "route-s"}
	router.AddRoute("route-s", func(*Message) error { return nil }, AckTypeProcessed, AckTypeNone, false, false)
	if delivered := router.DeliverAck(&Message{header: &_Header{
		command:   CommandAck,
		commandID: []byte("route-s"),
		subID:     []byte("route-s"),
		ackType:   &ackProcessed,
		status:    []byte("success"),
	}}, AckTypeProcessed); delivered == 0 {
		t.Fatalf("expected sub-id ack delivery")
	}
	router = &MessageRouter{routes: new(sync.Map), key: "route-q-only"}
	router.AddRoute("route-q-only", func(*Message) error { return nil }, AckTypeProcessed, AckTypeNone, false, false)
	if delivered := router.DeliverAck(&Message{header: &_Header{
		command: CommandAck,
		queryID: []byte("route-q-only"),
		ackType: &ackProcessed,
		status:  []byte("success"),
	}}, AckTypeProcessed); delivered == 0 {
		t.Fatalf("expected query-only ack delivery")
	}
	router = &MessageRouter{routes: new(sync.Map), key: "route-sub-only"}
	router.AddRoute("route-sub-only", func(*Message) error { return nil }, AckTypeProcessed, AckTypeNone, false, false)
	if delivered := router.DeliverAck(&Message{header: &_Header{
		command: CommandAck,
		subID:   []byte("route-sub-only"),
		ackType: &ackProcessed,
		status:  []byte("success"),
	}}, AckTypeProcessed); delivered == 0 {
		t.Fatalf("expected sub-only ack delivery")
	}

	var nilRouter *MessageRouter
	if route := nilRouter.FindRoute("x"); route != nil {
		t.Fatalf("expected nil router find route to return nil")
	}
	if delivered := nilRouter.deliverAck(&Message{header: new(_Header)}, AckTypeProcessed); delivered != 0 {
		t.Fatalf("expected nil router deliverAck to return zero")
	}
	nilRouter.processAckForRemoval(AckTypeProcessed, "x")

	router = &MessageRouter{routes: new(sync.Map), key: "missing"}
	if delivered := router.DeliverAck(&Message{header: &_Header{
		command:   CommandAck,
		commandID: []byte("missing"),
	}}, AckTypeProcessed); delivered != 0 {
		t.Fatalf("expected missing-route ack delivery count of zero")
	}
	if delivered := router.DeliverData(&Message{header: &_Header{
		command:   CommandPublish,
		commandID: []byte("missing"),
	}}); delivered != 0 {
		t.Fatalf("expected missing-route data delivery count of zero")
	}
	router = &MessageRouter{routes: new(sync.Map), key: "sub-data"}
	router.AddRoute("sub-data", func(*Message) error { return nil }, AckTypeNone, AckTypeNone, true, false)
	if delivered := router.DeliverData(&Message{header: &_Header{
		command: CommandPublish,
		subID:   []byte("sub-data"),
	}}); delivered == 0 {
		t.Fatalf("expected sub-id data delivery")
	}
	router.routes.Store("bad", 123)
	router.processAckForRemoval(AckTypeProcessed, "bad")
	if route := router.FindRoute("bad"); route != nil {
		t.Fatalf("expected non-function route lookup to return nil")
	}
}

func TestMessageRouterRouteMethodsCoverage(t *testing.T) {
	route := &MessageRoute{}
	called := 0
	handler := func(*Message) error {
		called++
		return nil
	}
	route.messageRoute(handler, AckTypeProcessed, AckTypeNone, false, false)

	if !route.isTerminationAck(AckTypeProcessed) {
		t.Fatalf("expected processed as termination ack")
	}
	if got := route.deliverAck(&Message{header: new(_Header)}, AckTypeProcessed); got != 1 {
		t.Fatalf("expected deliverAck to return 1, got %d", got)
	}
	if got := route.deliverAck(&Message{header: new(_Header)}, AckTypeReceived); got != 0 {
		t.Fatalf("expected deliverAck miss to return 0, got %d", got)
	}
	if got := route.deliverData(&Message{header: new(_Header)}); got != 1 {
		t.Fatalf("expected deliverData to return 1, got %d", got)
	}
	if route.getMessageHandler() == nil {
		t.Fatalf("expected route message handler")
	}
	if called < 2 {
		t.Fatalf("expected route callbacks to be invoked")
	}

	replaceRoute := &MessageRoute{}
	replaceRoute.messageRoute(func(*Message) error { return nil }, AckTypeNone, AckTypeNone, true, true)
	if replaceRoute.isTerminationAck(1) {
		t.Fatalf("subscribe route should not set termination ack")
	}

	// Error callback path in deliverAck/deliverData.
	errRoute := &MessageRoute{}
	errRoute.messageRoute(func(*Message) error { return NewError(CommandError, "boom") }, AckTypeProcessed, AckTypeNone, false, false)
	_ = errRoute.deliverAck(&Message{header: new(_Header)}, AckTypeProcessed)
	_ = errRoute.deliverData(&Message{header: new(_Header)})
}

func TestFixShredderFirstAndSubsequentFieldCoverage(t *testing.T) {
	shredder := NewFIXShredder()
	fixData := bytes.Join([][]byte{
		[]byte("8=FIX.4.4"),
		[]byte("35=D"),
		[]byte("49=SENDER"),
		[]byte(""),
	}, []byte{'\x01'})

	values := shredder.ToMap(fixData)
	if values[8] != "FIX.4.4" || values[35] != "D" || values[49] != "SENDER" {
		t.Fatalf("unexpected FIX parse map: %+v", values)
	}
}

func TestFixAndNVFixShredderMalformedCoverage(t *testing.T) {
	fixValues := NewFIXShredder('|').ToMap([]byte("broken|8=FIX.4.4|=bad|35=D|49=SENDER|"))
	if fixValues[8] != "FIX.4.4" || fixValues[35] != "D" || fixValues[49] != "SENDER" {
		t.Fatalf("unexpected FIX parse map with malformed segments: %+v", fixValues)
	}

	nvfixValues := NewNVFIXShredder('|').ToMap([]byte("broken|=bad|symbol=AAPL|venue=XNAS|"))
	if nvfixValues["symbol"] != "AAPL" || nvfixValues["venue"] != "XNAS" {
		t.Fatalf("unexpected NVFIX parse map with malformed segments: %+v", nvfixValues)
	}
}
