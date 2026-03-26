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
