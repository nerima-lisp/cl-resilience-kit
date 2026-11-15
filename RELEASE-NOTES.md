# v2.0.0

The core `cl-resilience-kit` system no longer depends on `cl-boundary-kit`.
Its runtime dependencies are `cl-concurrent-kit` 0.7.0 or newer and
`cl-date-kit` 1.0.0 or newer.

This release changes the objects accepted by the public `:clock`, `:sleeper`,
and `:random-source` injection keywords. Supply Common Lisp functions:

- A clock takes no arguments and returns time in the units selected by
  `:monotonic-units-per-second`.
- A sleeper takes one argument, the requested number of seconds.
- A random source takes an integer limit and returns an integer in
  `[0, limit)`.

The defaults use `get-internal-real-time`, `sleep`, and `random` with a fresh
private random state per retry policy. The clock's platform-dependent
monotonicity is unchanged. Old `cl-boundary-kit` objects are no longer accepted.
Pass the callback used by `make-clock` or `make-sleeper` directly, and capture
a private `random-state` in a function for jitter. See the migration example
in [README.md](README.md#migrating-from-1x).

Optional integrations remain explicit. The observability integration still
depends transitively on `cl-boundary-kit`, and the dataflow integration still
loads `cl-dataflow-kit`. Loading the core loads neither integration. The full
test suite continues to exercise both.
