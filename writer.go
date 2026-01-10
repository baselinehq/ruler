package ruler

import (
	"context"
	"net/http"

	remotewrite "github.com/baselinehq/remote-write"
	"github.com/prometheus/prometheus/prompb"
)

// SeriesWriter writes time series to remote storage.
type SeriesWriter interface {
	Write(ctx context.Context, series []prompb.TimeSeries) error
}

// RemoteWriteWriter adapts remotewrite.Client to SeriesWriter.
type RemoteWriteWriter struct {
	Client            *remotewrite.Client
	TenantID          string
	ExtraHeaders      http.Header
	MaxBatchBytes     int
	MaxSeriesPerBatch int
}

// Write sends time series via remote_write.
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
