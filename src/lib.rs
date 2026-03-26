//! `morb` is a lightweight in-process publish/subscribe library for Rust.
//!
//! It provides named topics, fixed-size message retention via a ring buffer,
//! and poll-based notifications built on `mio` and `eventfd`.

use std::collections::HashMap;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;

use mio::event::Source;
use mio::{Events, Poll, Token};

pub trait MorbDataType: Send + Sync + 'static + Clone {}
impl<T> MorbDataType for T where T: Send + Sync + 'static + Clone {}

/// Stores all globally registered topics.
pub struct TopicManager {
    topics: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    topic_num: usize,
}

static TOPIC_MANAGER: LazyLock<Arc<RwLock<TopicManager>>> =
    LazyLock::new(|| Arc::new(RwLock::new(TopicManager::new())));

/// Creates a new topic with the given name and queue size.
///
/// Returns an error if a topic with the same name already exists.
pub fn create_topic<T: MorbDataType>(
    name: String,
    queue_size: u16,
) -> Result<Arc<Topic<T>>, std::io::Error> {
    TOPIC_MANAGER
        .write()
        .unwrap()
        .create_topic(name, queue_size)
}

/// Returns a previously created topic by name and type.
pub fn get_topic<T: MorbDataType>(name: &str) -> Option<Arc<Topic<T>>> {
    TOPIC_MANAGER.read().unwrap().get_topic(name)
}

impl TopicManager {
    /// Creates an empty topic manager.
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            topic_num: 0,
        }
    }

    pub fn create_topic<T: MorbDataType>(
        &mut self,
        name: String,
        queue_size: u16,
    ) -> Result<Arc<Topic<T>>, std::io::Error> {
        if self.topics.contains_key(&name) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Topic already exists",
            ));
        }
        self.topic_num += 1;
        let topic = Arc::new(Topic::new(name.clone(), queue_size, self.topic_num));
        self.topics.insert(name, Box::new(topic.clone()));
        Ok(topic)
    }

    pub fn get_topic<T: MorbDataType>(&self, name: &str) -> Option<Arc<Topic<T>>> {
        self.topics
            .get(name)
            .and_then(|boxed| boxed.downcast_ref::<Arc<Topic<T>>>().cloned())
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Publisher<T: MorbDataType> {
    topic: Arc<Topic<T>>,
}

impl<T: MorbDataType> Publisher<T> {
    /// Publishes a value to the topic.
    pub fn publish(&self, data: T) {
        {
            let mut fifo = self.topic.fifo.lock().unwrap();
            let index = self.topic.generation.load(Ordering::Acquire) as usize
                % (self.topic.queue_size as usize);
            fifo[index] = Some(data);
            self.topic.generation.fetch_add(1, Ordering::AcqRel);
        }
        self.topic.notify();
    }
}

/// Reads messages from a topic in publish order.
pub struct Subscriber<T: MorbDataType> {
    topic: Arc<Topic<T>>,
    sub_generation: u32,
}

impl<T: MorbDataType> Subscriber<T> {
    /// Returns `true` if the subscriber has unread data.
    pub fn check_update(&self) -> bool {
        if self.sub_generation != self.topic.generation.load(Ordering::SeqCst) {
            return true;
        }
        false
    }

    /// Returns the next unread value, if any.
    ///
    /// If the subscriber has fallen behind the ring buffer, only retained
    /// messages are returned.
    pub fn check_update_and_copy(&mut self) -> Option<T> {
        let topic_generation = self.topic.generation.load(Ordering::SeqCst);
        if self.sub_generation == topic_generation {
            return None;
        }
        if self.sub_generation < topic_generation.saturating_sub(self.topic.queue_size as u32) {
            self.sub_generation = topic_generation.saturating_sub(self.topic.queue_size as u32);
        }
        let index = (self.sub_generation as usize) % (self.topic.queue_size as usize);
        self.sub_generation += 1;
        self.topic.fifo.lock().unwrap()[index].clone()
    }
}

/// Waits for updates from one or more topics.
pub struct TopicPoller {
    poll: Poll,
    events: Events,
}

impl Default for TopicPoller {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicPoller {
    /// Creates a new topic poller.
    pub fn new() -> Self {
        Self {
            poll: Poll::new().unwrap(),
            events: Events::with_capacity(1024),
        }
    }

    /// Registers a topic with the poller.
    pub fn add_topic<T: MorbDataType>(&mut self, topic: &Topic<T>) -> std::io::Result<()> {
        mio::unix::SourceFd(&topic.eventfd.as_raw_fd()).register(
            self.poll.registry(),
            topic.token,
            mio::Interest::READABLE,
        )
    }

    /// Removes a topic from the poller.
    pub fn remove_topic<T: MorbDataType>(&mut self, topic: &Topic<T>) -> std::io::Result<()> {
        mio::unix::SourceFd(&topic.eventfd.as_raw_fd()).deregister(self.poll.registry())
    }

    /// Waits until at least one registered topic becomes readable or the timeout expires.
    pub fn wait(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.poll.poll(&mut self.events, timeout)
    }

    /// Iterates over ready topic tokens from the last `wait` call.
    pub fn iter(&self) -> impl Iterator<Item = Token> + '_ {
        self.events.iter().map(|event| event.token())
    }
}

/// A named message channel with fixed-size retention and poll notifications.
pub struct Topic<T: MorbDataType> {
    name: String,
    fifo: Mutex<Vec<Option<T>>>,
    pub(crate) generation: AtomicU32,
    queue_size: u16,
    token: mio::Token,
    eventfd: OwnedFd,
}

impl<T: MorbDataType> Topic<T> {
    fn new(name: String, queue_size: u16, topic_id: usize) -> Self {
        assert!(queue_size > 0, "queue_size must be greater than 0");

        Self {
            name,
            fifo: Mutex::new(vec![None; queue_size as usize]),
            generation: AtomicU32::new(0),
            queue_size,
            token: Token(topic_id),
            eventfd: unsafe { OwnedFd::from_raw_fd(libc::eventfd(0, libc::EFD_NONBLOCK)) },
        }
    }

    fn notify(&self) {
        let value = usize::from(self.token) as u64;
        unsafe {
            libc::write(
                self.eventfd.as_raw_fd(),
                &value as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }

    /// Clears the pending poll notification for this topic.
    pub fn clear_event(&self) {
        let mut value: u64 = 0;
        unsafe {
            libc::read(
                self.eventfd.as_raw_fd(),
                &mut value as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the poll token associated with this topic.
    pub fn token(&self) -> Token {
        self.token
    }

    /// Creates a publisher for this topic.
    pub fn create_publisher(self: &Arc<Self>) -> Publisher<T> {
        Publisher {
            topic: self.clone(),
        }
    }

    /// Creates a subscriber for this topic.
    pub fn create_subscriber(self: &Arc<Self>) -> Subscriber<T> {
        Subscriber {
            topic: self.clone(),
            sub_generation: 0,
        }
    }
}

#[cfg(test)]
mod tests;
