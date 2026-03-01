package amps

import (
	"testing"
	"time"
)

var perfAPIStringSink string
var perfAPIBoolSink bool
var perfAPIDurationSink time.Duration
var perfAPILogonOptionsSink LogonParams
var perfAPIServerChooserSink ServerChooser
var perfAPIHAClientSink *HAClient

func BenchmarkAPIClientSetName(b *testing.B) {
	client := NewClient("api-client")
	values := [2]string{"api-client-a", "api-client-b"}
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		client.SetName(values[index&1])
	}
}

func BenchmarkAPIClientName(b *testing.B) {
	client := NewClient("api-client")
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIStringSink = client.Name()
	}
}

func BenchmarkAPIClientGetNameHash(b *testing.B) {
	client := NewClient("api-client")
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIStringSink = client.GetNameHash()
	}
}

func BenchmarkAPIClientSetLogonCorrelationData(b *testing.B) {
	client := NewClient("api-client")
	values := [2]string{"corr-a", "corr-b"}
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		client.SetLogonCorrelationData(values[index&1])
	}
}

func BenchmarkAPIClientGetLogonCorrelationData(b *testing.B) {
	client := NewClient("api-client")
	client.SetLogonCorrelationData("corr-data")
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIStringSink = client.GetLogonCorrelationData()
	}
}

func BenchmarkAPIClientSetAutoAck(b *testing.B) {
	client := NewClient("api-client")
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		client.SetAutoAck((index & 1) == 0)
	}
}

func BenchmarkAPIClientGetAutoAck(b *testing.B) {
	client := NewClient("api-client")
	client.SetAutoAck(true)
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIBoolSink = client.GetAutoAck()
	}
}

func BenchmarkAPIHASetTimeout(b *testing.B) {
	ha := NewHAClient("api-ha")
	values := [2]time.Duration{250 * time.Millisecond, 450 * time.Millisecond}
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		ha.SetTimeout(values[index&1])
	}
}

func BenchmarkAPIHATimeout(b *testing.B) {
	ha := NewHAClient("api-ha")
	ha.SetTimeout(250 * time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIDurationSink = ha.Timeout()
	}
}

func BenchmarkAPIHASetReconnectDelay(b *testing.B) {
	ha := NewHAClient("api-ha")
	values := [2]time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		ha.SetReconnectDelay(values[index&1])
	}
}

func BenchmarkAPIHAReconnectDelay(b *testing.B) {
	ha := NewHAClient("api-ha")
	ha.SetReconnectDelay(100 * time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIDurationSink = ha.ReconnectDelay()
	}
}

func BenchmarkAPIHASetReconnectDelayStrategy(b *testing.B) {
	ha := NewHAClient("api-ha")
	strategy := NewFixedDelayStrategy(100 * time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIHAClientSink = ha.SetReconnectDelayStrategy(strategy)
	}
}

func BenchmarkAPIHASetLogonOptions(b *testing.B) {
	ha := NewHAClient("api-ha")
	values := [2]LogonParams{{CorrelationID: "bench-a"}, {CorrelationID: "bench-b"}}
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		ha.SetLogonOptions(values[index&1])
	}
}

func BenchmarkAPIHALogonOptions(b *testing.B) {
	ha := NewHAClient("api-ha")
	ha.SetLogonOptions(LogonParams{CorrelationID: "bench-correlation"})
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPILogonOptionsSink = ha.LogonOptions()
	}
}

func BenchmarkAPIHASetServerChooser(b *testing.B) {
	ha := NewHAClient("api-ha")
	chooser := NewDefaultServerChooser("tcp://127.0.0.1:9000/amps/json")
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		ha.SetServerChooser(chooser)
	}
}

func BenchmarkAPIHAServerChooser(b *testing.B) {
	ha := NewHAClient("api-ha")
	ha.SetServerChooser(NewDefaultServerChooser("tcp://127.0.0.1:9000/amps/json"))
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIServerChooserSink = ha.ServerChooser()
	}
}

func BenchmarkAPIHADisconnected(b *testing.B) {
	ha := NewHAClient("api-ha")
	b.ReportAllocs()
	b.ResetTimer()

	for index := 0; index < b.N; index++ {
		perfAPIBoolSink = ha.Disconnected()
	}
}
