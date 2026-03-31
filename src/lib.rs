//! `morb` is a lightweight in-process publish/subscribe library for Rust.
//!
//! It provides named topics, fixed-size message retention via a ring buffer,
//! and poll-based notifications built on `mio` and `eventfd`.

use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;

use mio::event::Source;
use mio::{Events, Poll, Token};

pub trait MorbDataType: Send + Sync + 'static + Clone {}
impl<T> MorbDataType for T where T: Send + Sync + 'static + Clone {}

struct RingBuffer<T> {
    slots: Box<[MaybeUninit<T>]>,
    initialized: Box<[bool]>,
}

impl<T> RingBuffer<T> {
    fn new(size: usize) -> Self {
        let mut slots = Vec::with_capacity(size);
        slots.resize_with(size, MaybeUninit::uninit);

        Self {
            slots: slots.into_boxed_slice(),
            initialized: vec![false; size].into_boxed_slice(),
        }
    }

    fn write(&mut self, index: usize, value: T) {
        if self.initialized[index] {
            unsafe {
                self.slots[index].assume_init_drop();
            }
        }

        self.slots[index].write(value);
        self.initialized[index] = true;
    }

    fn read_cloned(&self, index: usize) -> Option<T>
    where
        T: Clone,
    {
        if !self.initialized[index] {
            return None;
        }

        Some(unsafe { self.slots[index].assume_init_ref().clone() })
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        for (index, initialized) in self.initialized.iter().copied().enumerate() {
            if initialized {
                unsafe {
                    self.slots[index].assume_init_drop();
                }
            }
        }
    }
}

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

/// Returns a previously created topic by name and type, or creates a new one if it doesn't exist.
///
/// If the topic already exists, `queue_size` is ignored.
pub fn get_or_create_topic<T: MorbDataType>(
    name: String,
    queue_size: u16,
) -> Result<Arc<Topic<T>>, std::io::Error> {
    if let Some(topic) = get_topic::<T>(&name) {
        return Ok(topic);
    }
    create_topic(name, queue_size)
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

#[derive(Clone)]
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
            fifo.write(index, data);
            self.topic.generation.fetch_add(1, Ordering::AcqRel);
        }
        self.topic.notify();
        if self.topic.blocking_waiters.load(Ordering::Acquire) > 0 {
            atomic_wait::wake_all(&self.topic.generation);
        }
    }
}

/// Reads messages from a topic in publish order.
#[derive(Clone)]
pub struct Subscriber<T: MorbDataType> {
    topic: Arc<Topic<T>>,
    sub_generation: u32,
}

struct BlockingWaitGuard {
    waiters: *const AtomicUsize,
}

impl BlockingWaitGuard {
    fn new(waiters: &AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::Release);
        Self {
            waiters: waiters as *const AtomicUsize,
        }
    }
}

impl Drop for BlockingWaitGuard {
    fn drop(&mut self) {
        unsafe {
            (*self.waiters).fetch_sub(1, Ordering::Release);
        }
    }
}

impl<T: MorbDataType> Subscriber<T> {
    /// Returns `true` if the subscriber has unread data.
    pub fn check_update(&self) -> bool {
        if self.sub_generation != self.topic.generation.load(Ordering::SeqCst) {
            return true;
        }
        false
    }

    /// Reads the next message, blocking until one is available.
    ///
    /// If there is already an unread message, returns it immediately.
    /// Otherwise, blocks until a new message is published.
    pub fn read_blocking(&mut self) -> T {
        let _wait_guard = BlockingWaitGuard::new(&self.topic.blocking_waiters);
        loop {
            let try_ret = self.check_update_and_copy();
            if try_ret.is_some() {
                return try_ret.unwrap();
            }
            atomic_wait::wait(&self.topic.generation, self.sub_generation);
        }
    }

    /// Reads the next message, blocking until one is available or the timeout expires.
    ///
    /// If there is already an unread message, returns it immediately.
    /// Otherwise, waits until a new message is published or the timeout expires.
    ///
    /// # Errors
    ///
    /// Returns an error with [`ErrorKind::TimedOut`] if no message is available
    /// within the specified timeout.
    pub fn read_timeout(&mut self, timeout: Duration) -> std::io::Result<T> {
        let start = std::time::Instant::now();
        let _wait_guard = BlockingWaitGuard::new(&self.topic.blocking_waiters);
        loop {
            if let Some(msg) = self.check_update_and_copy() {
                return Ok(msg);
            }

            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "read timed out",
                ));
            }

            let remaining = timeout - elapsed;
            let mut poller = TopicPoller::new();
            poller.add_topic(&self.topic)?;
            poller.wait(Some(remaining))?;
        }
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
        self.topic.fifo.lock().unwrap().read_cloned(index)
    }
}

/// Waits for updates from one or more topics.
pub struct TopicPoller {
    poll: Poll,
    events: Events,
    manual_events: Vec<Token>,
    registrations: Vec<TopicPollerRegistration>,
}

struct TopicPollerRegistration {
    eventfd: RawFd,
    token: Token,
    generation: *const AtomicU32,
    last_generation: u32,
    poller_count: Arc<AtomicUsize>,
    registered: bool,
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
            manual_events: Vec::new(),
            registrations: Vec::new(),
        }
    }

    /// Registers a topic with the poller.
    pub fn add_topic<T: MorbDataType>(&mut self, topic: &Topic<T>) -> std::io::Result<()> {
        self.registrations.push(TopicPollerRegistration {
            eventfd: topic.eventfd.as_raw_fd(),
            token: topic.token,
            generation: &topic.generation,
            last_generation: topic.generation.load(Ordering::SeqCst),
            poller_count: Arc::clone(&topic.poller_count),
            registered: false,
        });

        Ok(())
    }

    /// Removes a topic from the poller.
    pub fn remove_topic<T: MorbDataType>(&mut self, topic: &Topic<T>) -> std::io::Result<()> {
        if let Some(index) = self
            .registrations
            .iter()
            .position(|registration| registration.eventfd == topic.eventfd.as_raw_fd())
        {
            let registration = self.registrations.swap_remove(index);
            if registration.registered {
                mio::unix::SourceFd(&topic.eventfd.as_raw_fd()).deregister(self.poll.registry())?;
                registration.poller_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Waits until at least one registered topic becomes readable or the timeout expires.
    pub fn wait(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.manual_events.clear();

        if timeout == Some(Duration::ZERO) {
            for registration in &mut self.registrations {
                let generation = unsafe { (*registration.generation).load(Ordering::SeqCst) };
                if generation != registration.last_generation {
                    registration.last_generation = generation;
                    self.manual_events.push(registration.token);
                }
            }
            return Ok(());
        }

        for registration in &mut self.registrations {
            let generation = unsafe { (*registration.generation).load(Ordering::SeqCst) };
            if generation != registration.last_generation {
                registration.last_generation = generation;
                self.manual_events.push(registration.token);
            }
        }
        if !self.manual_events.is_empty() {
            return Ok(());
        }

        for registration in &mut self.registrations {
            if !registration.registered {
                mio::unix::SourceFd(&registration.eventfd).register(
                    self.poll.registry(),
                    registration.token,
                    mio::Interest::READABLE,
                )?;
                registration.poller_count.fetch_add(1, Ordering::Relaxed);
                registration.registered = true;
            }
        }

        self.poll.poll(&mut self.events, timeout)
    }

    /// Iterates over ready topic tokens from the last `wait` call.
    pub fn iter(&self) -> Box<dyn Iterator<Item = Token> + '_> {
        if !self.manual_events.is_empty() {
            Box::new(self.manual_events.iter().copied())
        } else {
            Box::new(self.events.iter().map(|event| event.token()))
        }
    }
}

impl Drop for TopicPoller {
    fn drop(&mut self) {
        for registration in self.registrations.drain(..) {
            if registration.registered {
                registration.poller_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// A named message channel with fixed-size retention and poll notifications.
pub struct Topic<T: MorbDataType> {
    name: String,
    fifo: Mutex<RingBuffer<T>>,
    pub(crate) generation: AtomicU32,
    queue_size: u16,
    token: mio::Token,
    eventfd: OwnedFd,
    poller_count: Arc<AtomicUsize>,
    blocking_waiters: AtomicUsize,
}

impl<T: MorbDataType> Topic<T> {
    fn new(name: String, queue_size: u16, topic_id: usize) -> Self {
        assert!(queue_size > 0, "queue_size must be greater than 0");

        Self {
            name,
            fifo: Mutex::new(RingBuffer::new(queue_size as usize)),
            generation: AtomicU32::new(0),
            queue_size,
            token: Token(topic_id),
            eventfd: unsafe { OwnedFd::from_raw_fd(libc::eventfd(0, libc::EFD_NONBLOCK)) },
            poller_count: Arc::new(AtomicUsize::new(0)),
            blocking_waiters: AtomicUsize::new(0),
        }
    }

    fn notify(&self) {
        if self.poller_count.load(Ordering::Relaxed) == 0 {
            return;
        }

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
