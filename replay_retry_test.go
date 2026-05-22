package ruler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestRetryChunk_SucceedsAfterTwoFailures(t *testing.T) {
	attempts := 0
	err := retryChunk(context.Background(), retryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, func() error {
		attempts++
		if attempts < 3 {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestRetryChunk_ExhaustsAttempts(t *testing.T) {
	attempts := 0
	err := retryChunk(context.Background(), retryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, func() error {
		attempts++
		return io.EOF
	})
	if err == nil {
		t.Fatal("want error after exhausting attempts")
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestRetryChunk_NonRetryableReturnsImmediately(t *testing.T) {
	attempts := 0
	err := retryChunk(context.Background(), retryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, func() error {
		attempts++
		return fmt.Errorf("400 Bad Request: bad query")
	})
	if err == nil {
		t.Fatal("want non-nil error")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (no retry)", attempts)
	}
}

func TestRetryChunk_CtxCancelStopsLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := retryChunk(ctx, retryConfig{MaxAttempts: 5, InitialDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}, func() error {
		attempts++
		return io.EOF
	})
	if !errors.Is(err, context.Canceled) && attempts > 2 {
		t.Errorf("attempts = %d, want few", attempts)
	}
}

func TestRetryChunk_ZeroMaxAttemptsCoercesToOne(t *testing.T) {
	attempts := 0
	err := retryChunk(context.Background(), retryConfig{MaxAttempts: 0, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, func() error {
		attempts++
		return io.EOF
	})
	if err == nil {
		t.Fatal("want non-nil error")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (coerced)", attempts)
	}
}

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{io.EOF, true},
		{context.DeadlineExceeded, true},
		{fmt.Errorf("connection reset by peer"), true},
		{fmt.Errorf("status=503 body=\"upstream\""), true},
		{fmt.Errorf("status=429 body=\"rate limit\""), true},
		{fmt.Errorf("status=400 body=\"parse error\""), false},
		{fmt.Errorf("status=404 body=\"not found\""), false},
	}
	for _, tc := range cases {
		got := isRetryable(tc.err)
		if got != tc.want {
			t.Errorf("isRetryable(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
