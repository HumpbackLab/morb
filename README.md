# morb

`morb` is a lightweight in-process publish/subscribe library for Rust. It provides named `Topic`s, a fixed-size ring buffer, and poll-based notifications built on `mio` and `eventfd`.

The current implementation targets Linux.

## Quick Start

```rust
use morb::{create_topic, TopicPoller};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let topic = create_topic::<u32>("numbers".to_string(), 16)?;
    let publisher = topic.create_publisher();
    let mut subscriber = topic.create_subscriber();

    publisher.publish(42);
    assert!(subscriber.check_update());
    assert_eq!(subscriber.check_update_and_copy(), Some(42));

    let mut poller = TopicPoller::new();
    poller.add_topic(&topic)?;

    publisher.publish(100);
    poller.wait(Some(Duration::from_millis(100)))?;

    for token in poller.iter() {
        if token == topic.token() {
            println!("{} updated", topic.name());
        }
    }

    Ok(())
}
```

Run tests:

```bash
cargo test
```

Run the benchmark example:

```bash
cargo run --release --example pubsub_benchmark -- 200000
```

Current sample results on this repository's benchmark setup (`-- 200000`):

```text
morb benchmark
iterations: 200000
queue_size: 1024
publish_only             200000 ops          7.35 ns/op  136029991.89 ops/s
publish_consume          200000 ops         11.19 ns/op   89325671.57 ops/s
publish_poll             200000 ops         18.69 ns/op   53502501.11 ops/s
multi_producer           200000 ops         51.76 ns/op   19320608.20 ops/s
multi_subscriber         200000 ops         37.32 ns/op   26796238.99 ops/s
large_msg_64             200000 ops         17.26 ns/op   57953477.27 ops/s
large_msg_256            200000 ops         37.53 ns/op   26647693.51 ops/s
large_msg_1024           200000 ops         75.61 ns/op   13225916.85 ops/s
blocking_poll             50000 ops      28460.42 ns/op      35136.52 ops/s
```

Notes:

- `multi_subscriber` runs 4 subscribers over 50,000 messages each, reported as 200,000 total subscriber reads.
- `blocking_poll` is capped at 50,000 waits to keep runtime practical.
- Results are machine-dependent and should be used for relative comparisons, not absolute guarantees.
