package ruler

import "errors"

// ErrNoWriter is returned when a SeriesWriter is required but not configured.
var ErrNoWriter = errors.New("no writer configured")
