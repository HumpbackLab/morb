use std::collections::HashSet;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use crate::{MorbDataType, Subscriber, TopicPoller};

/// Waits until one of the subscribers has a message available.
///
/// Returns the index of the ready subscriber in the input slice together with
/// the next message from that subscriber. If multiple subscribers are ready,
/// the first ready subscriber in slice order wins.
///
/// Passing `None` waits indefinitely. Passing `Some(Duration::ZERO)` performs
/// a non-blocking check and returns a timeout error if no message is ready.
pub fn select<T: MorbDataType>(
    subscribers: &mut [&mut Subscriber<T>],
    timeout: Option<Duration>,
) -> std::io::Result<(usize, T)> {
    if subscribers.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "select requires at least one subscriber",
        ));
    }

    let start = Instant::now();

    loop {
        for (index, subscriber) in subscribers.iter_mut().enumerate() {
            if let Some(msg) = subscriber.check_update_and_copy() {
                return Ok((index, msg));
            }
        }

        if let Some(timeout) = timeout {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "select timed out",
                ));
            }
        }

        let remaining = timeout.map(|limit| limit.saturating_sub(start.elapsed()));
        let mut poller = TopicPoller::new();
        let mut seen_eventfds = HashSet::new();
        for subscriber in subscribers.iter() {
            let eventfd = subscriber.topic.eventfd.as_raw_fd();
            if seen_eventfds.insert(eventfd) {
                poller.add_topic(&subscriber.topic)?;
            }
        }
        poller.wait(remaining)?;
    }
}
