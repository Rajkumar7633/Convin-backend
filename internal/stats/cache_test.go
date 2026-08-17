package stats_test

import (
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := stats.NewCache()
	const numGoroutines = 50
	const opsPerGoroutine = 100

	done := make(chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < opsPerGoroutine; j++ {
				c.Record("acc_shared", 10)
				_ = c.Get("acc_shared")
			}
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	got := c.Get("acc_shared")
	expectedCount := int64(numGoroutines * opsPerGoroutine)
	expectedDuration := expectedCount * 10
	if got.CallCount != expectedCount || got.TotalDurationSec != expectedDuration {
		t.Fatalf("concurrent cache corrupted: got CallCount=%d TotalDurationSec=%d, want CallCount=%d TotalDurationSec=%d",
			got.CallCount, got.TotalDurationSec, expectedCount, expectedDuration)
	}
}

