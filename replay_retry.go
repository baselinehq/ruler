package ruler

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strings"
	"time"
)

type retryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Jitter       float64
}

func defaultRetryConfig() retryConfig {
	return retryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		MaxDelay:     30 * time.Second,
		Jitter:       0.2,
	}
}

// retryChunk invokes op with bounded retries. Non-retryable errors return
// immediately. ctx cancellation interrupts the sleep between attempts.
func retryChunk(ctx context.Context, cfg retryConfig, op func() error) error {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	delay := cfg.InitialDelay
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return err
		}
		if attempt == cfg.MaxAttempts-1 {
			break
		}
		sleep := jitterDuration(delay, cfg.Jitter)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		delay *= 2
		if delay > cfg.MaxDelay && cfg.MaxDelay > 0 {
			delay = cfg.MaxDelay
		}
	}
	return lastErr
}

func jitterDuration(d time.Duration, frac float64) time.Duration {
	if frac <= 0 {
		return d
	}
	spread := float64(d) * frac
	return d + time.Duration(rand.Float64()*spread-spread/2)
}

// isRetryable returns true for transient errors worth retrying.
// Retryable: I/O, deadline, connection-level errors, 5xx, 429.
// Not retryable: 4xx (except 429), parse/validation errors.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "connection reset") || strings.Contains(lower, "connection refused") || strings.Contains(lower, "broken pipe") {
		return true
	}
	if strings.Contains(msg, "status=5") || strings.Contains(msg, "status=429") {
		return true
	}
	if strings.Contains(msg, "status=4") {
		return false
	}
	return false
}
