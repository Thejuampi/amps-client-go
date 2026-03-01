package main

import "testing"

func TestViewFlagsSetAndString(t *testing.T) {
	var v viewFlags
	if err := v.Set("a:b"); err != nil {
		t.Fatalf("unexpected error from viewFlags.Set: %v", err)
	}
	if len(v) != 1 || v[0] != "a:b" {
		t.Fatalf("unexpected viewFlags value: %v", v)
	}
	if v.String() == "" {
		t.Fatalf("expected non-empty String()")
	}
}

func TestActionFlagsSetAndString(t *testing.T) {
	var a actionFlags
	if err := a.Set("on-publish:orders:log"); err != nil {
		t.Fatalf("unexpected error from actionFlags.Set: %v", err)
	}
	if len(a) != 1 || a[0] != "on-publish:orders:log" {
		t.Fatalf("unexpected actionFlags value: %v", a)
	}
	if a.String() == "" {
		t.Fatalf("expected non-empty String()")
	}
}
