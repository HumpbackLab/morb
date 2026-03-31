# Release Notes

## v0.2.0

- Added the benchmark example in `examples/pubsub_benchmark.rs`.
- Added CI via `.github/workflows/ci.yml`.
- Improved poll performance with the zero-timeout `manual_events` fast path and lazy epoll registration.
- Refactored the ring buffer to use `MaybeUninit` and tightened poller registration handling.
- Expanded README usage notes and benchmark coverage.

## v0.1.0

- Initial tagged release.
