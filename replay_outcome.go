package ruler

// ReplayOutcome captures the terminal state of a replayRunner.
// Used by downstream runners to decide cascade-skip behavior.
type ReplayOutcome int

const (
	OutcomePending ReplayOutcome = iota
	OutcomeCompleted
	OutcomeSkippedAlreadyBackfilled
	OutcomeSkippedNoSpan
	OutcomeSkippedNoSource
	OutcomeSkippedDisabled
	OutcomeSkippedRetention
	OutcomeCycle
	OutcomeFailed
	OutcomeCancelled
)

func (o ReplayOutcome) String() string {
	switch o {
	case OutcomeCompleted:
		return "completed"
	case OutcomeSkippedAlreadyBackfilled:
		return "skipped_already_backfilled"
	case OutcomeSkippedNoSpan:
		return "skipped_no_span"
	case OutcomeSkippedNoSource:
		return "skipped_no_source"
	case OutcomeSkippedDisabled:
		return "skipped_disabled"
	case OutcomeSkippedRetention:
		return "skipped_retention"
	case OutcomeCycle:
		return "cycle"
	case OutcomeFailed:
		return "failed"
	case OutcomeCancelled:
		return "cancelled"
	default:
		return "pending"
	}
}

func (o ReplayOutcome) IsSuccess() bool {
	return o == OutcomeCompleted || o == OutcomeSkippedAlreadyBackfilled
}
