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

#[doc(hidden)]
#[macro_export]
macro_rules! __morb_select_dispatch {
    ($index:expr, $msg:ident, $current:expr; $bind:pat = $sub:expr => $body:block $(,)?) => {{
        debug_assert_eq!($index, $current);
        let $bind = $msg;
        Ok($body)
    }};
    ($index:expr, $msg:ident, $current:expr; $bind:pat = $sub:expr => $body:block, $($rest:tt)+) => {{
        if $index == $current {
            let $bind = $msg;
            Ok($body)
        } else {
            $crate::__morb_select_dispatch!($index, $msg, $current + 1usize; $($rest)+)
        }
    }};
}

/// Selects the first ready subscriber and dispatches to the matching branch.
///
/// The macro evaluates to `std::io::Result<T>`, where `T` is the branch result type.
///
/// ```
/// # use morb::*;
/// # let topic1 = create_topic("t1".into(), 10).unwrap();
/// # let topic2 = create_topic("t2".into(), 10).unwrap();
/// # let mut sub1 = topic1.create_subscriber();
/// # let mut sub2 = topic2.create_subscriber();
/// # topic2.create_publisher().publish(7_u32);
/// let result = select! {
///     msg = sub1 => { msg + 1 },
///     msg = sub2 => { msg + 2 },
/// };
/// assert_eq!(result.unwrap(), 9);
/// ```
///
/// A timeout can be provided as the first argument before a semicolon:
///
/// ```
/// # use morb::*;
/// # use std::time::Duration;
/// # let topic = create_topic::<u32>("t3".into(), 10).unwrap();
/// # let mut sub = topic.create_subscriber();
/// let result = select!(Some(Duration::from_millis(0)); msg = sub => { msg });
/// assert!(result.is_err());
/// ```
#[macro_export]
macro_rules! select {
    ($($bind:pat = $sub:expr => $body:block),+ $(,)?) => {{
        $crate::select!(None::<std::time::Duration>; $($bind = $sub => $body),+)
    }};
    ($timeout:expr; $($bind:pat = $sub:expr => $body:block),+ $(,)?) => {{
        let mut __morb_subscribers = [$( &mut $sub ),+];
        match $crate::select(&mut __morb_subscribers, $timeout) {
            Ok((__morb_index, __morb_msg)) => {
                $crate::__morb_select_dispatch!(__morb_index, __morb_msg, 0usize; $($bind = $sub => $body),+)
            }
            Err(__morb_err) => Err(__morb_err),
        }
    }};
}
