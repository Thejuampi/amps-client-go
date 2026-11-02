package main

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestCPPQueueRedeliveryTargetUsesConnectionIdentity(t *testing.T) {
	var tests = []struct {
		name           string
		alternateID    string
		thirdID        string
		pauseOriginal  bool
		pullOriginal   bool
		filterOriginal string
		fullOriginal   bool
		wantOriginal   bool
		wantAlternate  bool
	}{
		{name: "only subscriber", wantOriginal: true},
		{name: "same ID on another connection", alternateID: "shared", thirdID: "z-other", wantAlternate: true},
		{name: "different ID on another connection", alternateID: "z-other", wantAlternate: true},
		{name: "paused only subscriber", pauseOriginal: true},
		{name: "pull only subscriber", pullOriginal: true},
		{name: "nonmatching only subscriber", filterOriginal: `/id = 2`},
		{name: "full only subscriber", fullOriginal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var topic = "queue://cpp-redelivery-" + test.name
			defer func() {
				queueRedeliveryMu.Lock()
				delete(queueRedeliveryCursor, topic)
				queueRedeliveryMu.Unlock()
			}()
			var owner, ownerPeer = net.Pipe()
			defer owner.Close()
			defer ownerPeer.Close()
			var original = &subscription{conn: owner, subID: "shared", topic: topic, isQueue: true, pullMode: test.pullOriginal, filter: test.filterOriginal}
			original.paused.Store(test.pauseOriginal)
			registerSubscription(topic, original)
			defer unregisterSubscription(topic, original)
			if test.fullOriginal {
				original.maxBacklog = 1
				addQueueLeaseForOwner(owner, topic, original.subID, "cpp-full-key", "full-bookmark", nil, "", "json", time.Second)
				defer removeQueueLease("cpp-full-key")
			}

			var alternate *subscription
			if test.alternateID != "" {
				var other, otherPeer = net.Pipe()
				defer other.Close()
				defer otherPeer.Close()
				alternate = &subscription{conn: other, subID: test.alternateID, topic: topic, isQueue: true}
				registerSubscription(topic, alternate)
				defer unregisterSubscription(topic, alternate)
			}
			if test.thirdID != "" {
				var third, thirdPeer = net.Pipe()
				defer third.Close()
				defer thirdPeer.Close()
				var thirdSub = &subscription{conn: third, subID: test.thirdID, topic: topic, isQueue: true}
				registerSubscription(topic, thirdSub)
				defer unregisterSubscription(topic, thirdSub)
			}
			var lease = &queueLease{topic: topic, subID: "shared", ownerConn: owner, payload: []byte(`{"id":1}`)}
			var want *subscription
			if test.wantOriginal {
				want = original
			} else if test.wantAlternate {
				want = alternate
			}
			if got := selectQueueRedeliveryTarget(lease); got != want {
				t.Fatalf("redelivery target = %p, want %p", got, want)
			}
		})
	}
}

func TestCPPQueueRequeuesExpiredLeaseToOnlySubscriber(t *testing.T) {
	var topic = "queue://cpp-redelivery-single"
	var owner, peer = net.Pipe()
	var writer = &connWriter{ch: make(chan []byte, 2)}
	var sub = &subscription{conn: owner, subID: "one", topic: topic, isQueue: true, writer: writer, maxBacklog: 1}
	registerSubscription(topic, sub)
	defer func() {
		unregisterSubscription(topic, sub)
		removeQueueLease("cpp-redelivery-key")
		queuePendingMu.Lock()
		delete(queuePending, topic)
		queuePendingMu.Unlock()
		_ = peer.Close()
		_ = owner.Close()
	}()

	var lease = &queueLease{
		topic: topic, subID: sub.subID, ownerConn: owner, sowKey: "cpp-redelivery-key",
		bookmark: "bookmark-1", payload: []byte(`{"id":1}`), timestamp: "ts", messageType: "json", leasePeriod: time.Second,
	}
	requeueMessage(lease)
	if pending := popPendingQueueMessage(topic); pending != nil {
		t.Fatalf("eligible subscriber left message pending: %#v", pending)
	}
	var renewed = getQueueLease(lease.sowKey)
	if renewed == nil || renewed.ownerConn != owner || renewed.bookmark != lease.bookmark || renewed.leasePeriod != lease.leasePeriod {
		t.Fatalf("renewed lease = %#v", renewed)
	}
	if count := len(writer.ch); count != 1 {
		t.Fatalf("queued %d redelivery frames, want exactly one", count)
	}
	var frame = <-writer.ch
	var expected = `{"c":"p","t":"queue://cpp-redelivery-single","sub_id":"one","sids":"one","bm":"bookmark-1","ts":"ts","k":"cpp-redelivery-key","mt":"json","lp":"` + strconv.FormatInt(flagLease.Milliseconds(), 10) + `ms"}{"id":1}`
	if got := string(frame[4:]); got != expected {
		t.Fatalf("redelivery frame = %q, want %q", got, expected)
	}
}
