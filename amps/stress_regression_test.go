package amps

import (
	"testing"
	"time"
)

func TestClosedAckWaitDoesNotStartWarningWatcher(t *testing.T) {
	var client = NewClient("processed-ack-closed")
	client.setSyncAckProcessing(make(chan _Result, 1))
	client.closeSyncAckProcessing()
	var done = client.startSyncAckWarning(NewCommand("sow_and_subscribe"), "7", "7")
	if done != nil {
		closeSignal(done)
		t.Fatal("warning watcher started after its ack wait was closed")
	}
}

func TestProcessedAckReceiptStopsWarningsBeforeWaiterResumes(t *testing.T) {
	for _, receivedBeforeWarning := range []bool{false, true} {
		var name = "after_timer_start"
		if receivedBeforeWarning {
			name = "before_timer_start"
		}
		t.Run(name, func(t *testing.T) {
			var client = NewClient("processed-ack-receipt")
			var processing = make(chan _Result, 1)
			client.setSyncAckProcessing(processing)
			defer client.closeSyncAckProcessing()
			var warnings = make(chan error, 4)
			client.SetErrorHandler(func(err error) { warnings <- err })
			var acknowledge = func() {
				if !client.signalSyncAckProcessing(_Result{Status: true}) {
					t.Fatal("processed ack could not be queued")
				}
			}
			if receivedBeforeWarning {
				acknowledge()
			}
			var done = client.startSyncAckWarning(NewCommand("sow_and_subscribe"), "7", "7")
			defer closeSignal(done)
			if !receivedBeforeWarning {
				acknowledge()
			}
			// Leave the result queued, as when the waiting goroutine has not
			// been scheduled after the read loop received the processed ack.
			select {
			case warning := <-warnings:
				t.Fatalf("warning after processed ack receipt: %v", warning)
			case <-time.After(2 * syncAckWarningInitialDelay):
			}
			if len(processing) != 1 {
				t.Fatal("warning watcher consumed the processed ack")
			}
		})
	}
}

func TestPreviousProcessedAckDoesNotSuppressNextCommandWarning(t *testing.T) {
	var client = NewClient("processed-ack-next-command")
	client.setSyncAckProcessing(make(chan _Result, 1))
	if !client.signalSyncAckProcessing(_Result{Status: true}) {
		t.Fatal("first processed ack could not be queued")
	}
	client.closeSyncAckProcessing()
	client.setSyncAckProcessing(make(chan _Result, 1))
	defer client.closeSyncAckProcessing()
	var warnings = make(chan error, 4)
	client.SetErrorHandler(func(err error) { warnings <- err })
	var done = client.startSyncAckWarning(NewCommand("sow_and_subscribe"), "8", "8")
	defer closeSignal(done)
	select {
	case <-warnings:
	case <-time.After(2 * syncAckWarningInitialDelay):
		t.Fatal("the previous ack suppressed the unacknowledged command's warning")
	}
}

func TestProcessedAckWarningDoesNotFollowReplacementWait(t *testing.T) {
	var client = NewClient("processed-ack-replaced")
	client.setSyncAckProcessing(make(chan _Result, 1))
	var warnings = make(chan error, 4)
	client.SetErrorHandler(func(err error) { warnings <- err })
	var done = client.startSyncAckWarning(NewCommand("sow_and_subscribe"), "7", "7")
	defer closeSignal(done)
	if !client.signalSyncAckProcessing(_Result{Status: true}) {
		t.Fatal("processed ack could not be queued")
	}
	client.closeSyncAckProcessing()
	client.setSyncAckProcessing(make(chan _Result, 1))
	defer client.closeSyncAckProcessing()
	select {
	case warning := <-warnings:
		t.Fatalf("previous command's watcher followed its replacement: %v", warning)
	case <-time.After(2 * syncAckWarningInitialDelay):
	}
}

func BenchmarkSyncAckReceipt(b *testing.B) {
	var client = NewClient("sync-ack-benchmark")
	var processing = make(chan _Result, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		client.setSyncAckProcessing(processing)
		if !client.signalSyncAckProcessing(_Result{Status: true}) {
			b.Fatal("processed ack could not be queued")
		}
		<-processing
	}
}
