package ruler

import (
	"context"
	"errors"
	"testing"

	remotewrite "github.com/baselinehq/remote-write"
	"github.com/prometheus/prometheus/prompb"
)

// Compile-time guarantees the interface stays narrow and that both client
// flavors satisfy it.
var (
	_ Pusher = (*remotewrite.Client)(nil)
	_ Pusher = (*remotewrite.DurableClient)(nil)
)

type stubPusher struct {
	calls   int
	lastReq remotewrite.PushTimeSeriesRequest
	err     error
}

func (s *stubPusher) PushTimeSeries(ctx context.Context, req remotewrite.PushTimeSeriesRequest) error {
	s.calls++
	s.lastReq = req
	return s.err
}

func TestRemoteWriteWriter_DelegatesToPusher(t *testing.T) {
	stub := &stubPusher{}
	w := &RemoteWriteWriter{
		Client:        stub,
		TenantID:      "tenant-a",
		MaxBatchBytes: 1024,
	}
	tss := []prompb.TimeSeries{{Labels: []prompb.Label{{Name: "__name__", Value: "foo"}}}}
	if err := w.Write(context.Background(), tss); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if stub.calls != 1 {
		t.Errorf("calls = %d, want 1", stub.calls)
	}
	if stub.lastReq.TenantID != "tenant-a" {
		t.Errorf("TenantID = %q, want tenant-a", stub.lastReq.TenantID)
	}
	if stub.lastReq.MaxBatchBytes != 1024 {
		t.Errorf("MaxBatchBytes = %d, want 1024", stub.lastReq.MaxBatchBytes)
	}
	if len(stub.lastReq.TimeSeries) != 1 {
		t.Errorf("TimeSeries len = %d, want 1", len(stub.lastReq.TimeSeries))
	}
}

func TestRemoteWriteWriter_NilClientErrors(t *testing.T) {
	w := &RemoteWriteWriter{Client: nil}
	err := w.Write(context.Background(), nil)
	if !errors.Is(err, ErrNoWriter) {
		t.Errorf("err = %v, want ErrNoWriter", err)
	}
}

func TestRemoteWriteWriter_PropagatesPusherError(t *testing.T) {
	wantErr := errors.New("boom")
	stub := &stubPusher{err: wantErr}
	w := &RemoteWriteWriter{Client: stub}
	if err := w.Write(context.Background(), nil); !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}
