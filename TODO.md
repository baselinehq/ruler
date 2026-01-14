# TODO: Recording rule dependencies

Problem
- Recording rules query a remote TSDB and write via remote_write.
- Rules in the same group can depend on other recorded series.
- Remote writes are not guaranteed to be immediately queryable, so dependent rules can see missing data.
- Concurrency > 1 makes this worse, but the issue exists even with concurrency=1.

Workarounds in rules
- Inline raw metrics instead of chaining recorded rules.
- Use offset on dependent rules to read the previous evaluation.
- Split into groups and use eval_offset/eval_delay to enforce ordering.

Potential library improvements
- Dependency-aware evaluation with in-memory results for same-eval queries.
- Optional read-your-writes overlay cache for instant queries.
- Configurable post-write delay or poll before dependent rules.
- Detect and flag same-group dependencies in config validation.
