package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestRecordingProcessed_BackgroundContext(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := eventJSON(eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Wait for background recording processing (50ms sleep in service.go)
	time.Sleep(150 * time.Millisecond)

	var processed bool
	err := st.Pool().QueryRow(context.Background(),
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
	if err != nil {
		t.Fatalf("query recording_processed: %v", err)
	}
	if !processed {
		t.Fatalf("expected recording_processed to be true for call %s", callID)
	}
}

func TestConcurrentDuplicateEvents_NoDoubleCounting(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := eventJSON(eventID, callID, accountID)
	const concurrentRequests = 10

	var wg sync.WaitGroup
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = post(t, srv.URL+"/webhooks/calls", body)
		}()
	}
	wg.Wait()

	// Verify exact 1 event was stored
	var eventCount int
	err := st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("stored %d event records for %s, want 1", eventCount, eventID)
	}

	// Verify account stats call_count is exactly 1 (not 10!)
	statsResult, err := st.AccountStats(context.Background(), accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if statsResult.CallCount != 1 {
		t.Fatalf("account %s call_count = %d, want 1", accountID, statsResult.CallCount)
	}
}

func TestDifferentEventsSameCall_NoDoubleCounting(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, callID, accountID := testutil.IDs(t, st)

	body1 := eventJSON("evt_retry_1", callID, accountID)
	resp1 := post(t, srv.URL+"/webhooks/calls", body1)
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp1.StatusCode)
	}

	body2 := eventJSON("evt_retry_2", callID, accountID)
	resp2 := post(t, srv.URL+"/webhooks/calls", body2)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp2.StatusCode)
	}

	statsResult, err := st.AccountStats(context.Background(), accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if statsResult.CallCount != 1 {
		t.Fatalf("account %s call_count = %d, want 1 for single call_id", accountID, statsResult.CallCount)
	}
}



