package ruler

import (
	"context"
	"net/http"

	remotewrite "github.com/baselinehq/remote-write"
	"github.com/prometheus/prometheus/prompb"
)

// Pusher is the minimum contract RemoteWriteWriter requires from the
// underlying remote_write client. Both *remotewrite.Client and
// *remotewrite.DurableClient satisfy this interface.
//
// DurableClient adds an on-disk spool plus a background drain goroutine for
// resilience against transient remote_write failures and rate limits. Callers
// wiring up a DurableClient are responsible for invoking DurableClient.Run(ctx)
// in a background goroutine before passing it here, and for invoking
// DurableClient.Close() during shutdown. ruler does not own that lifecycle.
type Pusher interface {
	PushTimeSeries(ctx context.Context, req remotewrite.PushTimeSeriesRequest) error
}

// SeriesWriter writes time series to remote storage.
type SeriesWriter interface {
	Write(ctx context.Context, series []prompb.TimeSeries) error
}

// RemoteWriteWriter adapts a Pusher (Client or DurableClient) to SeriesWriter.
type RemoteWriteWriter struct {
	Client            Pusher
	TenantID          string
	ExtraHeaders      http.Header
	MaxBatchBytes     int
	MaxSeriesPerBatch int
}

// Write sends time series via remote_write through the configured Pusher.
func (w *RemoteWriteWriter) Write(ctx context.Context, series []prompb.TimeSeries) error {
	if w == nil || w.Client == nil {
		return ErrNoWriter
	}
	return w.Client.PushTimeSeries(ctx, remotewrite.PushTimeSeriesRequest{
		TenantID:          w.TenantID,
		TimeSeries:        series,
		MaxBatchBytes:     w.MaxBatchBytes,
		MaxSeriesPerBatch: w.MaxSeriesPerBatch,
		ExtraHeaders:      w.ExtraHeaders,
	})
}
