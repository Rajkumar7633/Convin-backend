// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store  *store.Store
	cache  *stats.Cache
	rdb    *redis.Client
	log    *slog.Logger
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		store:  s,
		cache:  c,
		rdb:    rdb,
		log:    log,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Stats returns the cached totals for an account, falling back to durable store on cold cache.
func (s *Service) Stats(accountID string) stats.AccountStats {
	cached := s.cache.Get(accountID)
	if cached.CallCount > 0 {
		return cached
	}

	// Cold cache fallback: load from persistent store if available
	dbStats, err := s.store.AccountStats(context.Background(), accountID)
	if err == nil && dbStats.CallCount > 0 {
		st := stats.AccountStats{
			CallCount:        dbStats.CallCount,
			TotalDurationSec: dbStats.TotalDurationSec,
		}
		s.cache.Set(accountID, st)
		return st
	}
	return cached
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	newlyIngested, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !newlyIngested {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	// Use background service context and track with waitgroup so in-flight work
	// is not aborted when the HTTP request context finishes or on server shutdown.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func(eventRec store.Event) {
			defer s.wg.Done()
			if err := s.processRecording(s.ctx, eventRec); err != nil {
				s.log.Error("process recording failed", "call_id", eventRec.CallID, "err", err)
			}
		}(rec)
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	select {
	case <-time.After(recordingWork):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// Shutdown gracefully waits for all in-flight asynchronous tasks to finish.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.cancel()
		return nil
	case <-ctx.Done():
		s.cancel()
		return ctx.Err()
	}
}

