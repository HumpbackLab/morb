use std::collections::HashMap;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;

use mio::{Events, Poll, Token};
use mio::event::Source;

pub trait MorbDataType: Send + Sync + 'static + Clone {}
impl<T> MorbDataType for T where T: Send + Sync + 'static + Clone {}

pub struct TopicManager {
    topics: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    topic_num: usize,
}

static TOPIC_MANAGER: LazyLock<Arc<RwLock<TopicManager>>> =
    LazyLock::new(|| Arc::new(RwLock::new(TopicManager::new())));

pub fn create_topic<T: MorbDataType>(name: String, queue_size: u16) -> Arc<Topic<T>> {
    TOPIC_MANAGER
        .write()
        .unwrap()
        .create_topic(name, queue_size)
}

pub fn get_topic<T: MorbDataType>(name: &str) -> Option<Arc<Topic<T>>> {
    TOPIC_MANAGER.read().unwrap().get_topic(name)
}

impl TopicManager {
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
    ) -> Arc<Topic<T>> {
        self.topic_num += 1;
        let topic = Arc::new(Topic::new(name.clone(), queue_size, self.topic_num));
        assert!(self.topics.insert(name, Box::new(topic.clone())).is_none(), "topic with the same name already exists");
        topic
    }

    pub fn get_topic<T: MorbDataType>(&self, name: &str) -> Option<Arc<Topic<T>>> {
        self.topics
            .get(name)
            .and_then(|boxed| boxed.downcast_ref::<Arc<Topic<T>>>().cloned())
    }
}

pub struct Publisher<T: MorbDataType> {
    topic: Arc<Topic<T>>,
}

impl<T: MorbDataType> Publisher<T> {
    pub fn publish(&self, data: T) {
        {
            let mut fifo = self.topic.fifo.lock().unwrap();
            let index = self.topic.generation.load(Ordering::Acquire) as usize % (self.topic.queue_size as usize);
            fifo[index] = Some(data);
        }
        self.topic.generation.fetch_add(1, Ordering::AcqRel);        
        self.topic.notify();
    }
}

pub struct Subscriber<T: MorbDataType> {
    topic: Arc<Topic<T>>,
    sub_generation: u32,
}

impl<T: MorbDataType> Subscriber<T> {
    pub fn check_update(&self) -> bool {
        if self.sub_generation != self.topic.generation.load(Ordering::SeqCst) {
            return true;
        }
        false
    }

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

pub struct TopicPoller {
    poll: Poll,
    events: Events,
}

impl TopicPoller {
    pub fn new() -> Self {
        Self {
            poll: Poll::new().unwrap(),
            events: Events::with_capacity(1024),
        }
    }

    pub fn add_topic<T: MorbDataType>(&mut self, topic: &Topic<T>) -> std::io::Result<()> {
        mio::unix::SourceFd(&topic.eventfd.as_raw_fd()).register(
            self.poll.registry(),
            topic.token,
            mio::Interest::READABLE,
        )
    }

    pub fn remove_topic<T: MorbDataType>(&mut self, topic: &Topic<T>) -> std::io::Result<()> {
        mio::unix::SourceFd(&topic.eventfd.as_raw_fd()).deregister(
            self.poll.registry(),
        )
    }

    pub fn wait(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.poll.poll(&mut self.events, timeout)
    }

    pub fn iter(&self) -> impl Iterator<Item = Token> + '_ {
        self.events.iter().map(|event| event.token())
    }
}

pub struct Topic<T: MorbDataType> {
    name: String,
    fifo: Mutex<Vec<Option<T>>>,
    pub(crate) generation: AtomicU32,
    queue_size: u16,
    pub(crate) token: mio::Token,
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
        let value = usize::from(self.token);
        unsafe {
            libc::write(
                self.eventfd.as_raw_fd(),
                &value as *const usize as *const libc::c_void,
                std::mem::size_of::<usize>(),
            );
        }
    }

    pub fn clear_event(&self) {
        let mut value: usize = 0;
        unsafe {
            libc::read(
                self.eventfd.as_raw_fd(),
                &mut value as *mut usize as *mut libc::c_void,
                std::mem::size_of::<usize>(),
            );
        }
    }

    pub fn create_publisher(self: &Arc<Self>) -> Publisher<T> {
        Publisher {
            topic: self.clone(),
        }
    }

    pub fn create_subscriber(self: &Arc<Self>) -> Subscriber<T> {
        Subscriber {
            topic: self.clone(),
            sub_generation: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicBool;
    use std::sync::Barrier;
    use std::thread;

    #[derive(Clone, Debug, PartialEq)]
    struct TestStruct {
        test: u32,
        test2: u64,
    }

    #[test]
    fn basic_topic_pub_sub_test() {
        let test_topic = create_topic::<TestStruct>("test_struct_basic_poll".to_string(), 16);
        let publisher_struct = test_topic.create_publisher();
        publisher_struct.publish(TestStruct { test: 1, test2: 2 });
        assert_eq!(test_topic.generation.load(Ordering::SeqCst), 1);

        let publisher = test_topic.create_publisher();
        publisher.publish(TestStruct {
            test: 42,
            test2: 43,
        });
        assert_eq!(test_topic.generation.load(Ordering::SeqCst), 2);

        let mut sub = test_topic.create_subscriber();
        assert!(sub.check_update());
        let data = sub.check_update_and_copy();
        assert!(data.is_some());
        let data = data.unwrap();
        assert_eq!(data.test, 1);
        assert_eq!(data.test2, 2);

        assert!(sub.check_update());
        let data = sub.check_update_and_copy();
        assert!(data.is_some());
        let data = data.unwrap();
        assert_eq!(data.test, 42);
        assert_eq!(data.test2, 43);
    }

    #[test]
    fn topic_test() {
        let topic: Topic<u16> = Topic::new("test".to_string(), 16, 1);
        assert_eq!(topic.name, "test");
        assert_eq!(topic.generation.load(Ordering::SeqCst), 0);
        assert_eq!(topic.token, Token(1));
    }

    #[test]
    fn topic_poller_add_topic_test() {
        let mut poller = TopicPoller::new();
        let topic: Topic<u32> = Topic::new("test_poll".to_string(), 16, 2);

        let result = poller.add_topic(&topic);
        assert!(result.is_ok());
    }

    #[test]
    fn topic_poller_wait_test() {
        let mut poller = TopicPoller::new();

        // Add topic to poller - no need for mut anymore!
        let reg_topic: Topic<u32> = Topic::new("test_wait".to_string(), 16, 3);
        poller.add_topic(&reg_topic).unwrap();

        // Wait with timeout should return Ok even with no events
        let result = poller.wait(Some(Duration::from_millis(10)));
        assert!(result.is_ok());
    }

    #[test]
    fn topic_poller_notification_test() {
        let mut poller = TopicPoller::new();

        // Create a topic and wrap in Arc for sharing
        let topic = Arc::new(Topic::new("test_notify".to_string(), 16, 4));

        // Add topic to poller - no need for separate instance!
        poller.add_topic(&topic).unwrap();

        // Clone Arc for the thread
        let topic_clone = topic.clone();

        // Spawn a thread to publish after a short delay
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let publisher = topic_clone.create_publisher();
            publisher.publish(42u32);
        });

        // Wait for notification with timeout
        let result = poller.wait(Some(Duration::from_millis(200)));
        assert!(result.is_ok());

        for i in poller.iter() {
            assert_eq!(i, topic.token);
        }

        handle.join().unwrap();
    }

    #[test]
    fn topic_notify_and_clear_test() {
        let topic: Topic<u32> = Topic::new("test_clear".to_string(), 16, 5);

        // Notify
        topic.notify();

        // Clear the event
        topic.clear_event();

        // Create another notification
        topic.notify();
        topic.clear_event();
    }

    #[test]
    fn topic_poller_iter_test() {
        let mut poller = TopicPoller::new();
        let topic1: Topic<u32> = Topic::new("test_iter1".to_string(), 16, 6);
        let topic2: Topic<u32> = Topic::new("test_iter2".to_string(), 16, 7);

        poller.add_topic(&topic1).unwrap();
        poller.add_topic(&topic2).unwrap();

        // Wait with timeout
        poller.wait(Some(Duration::from_millis(10))).unwrap();

        // Iterate over events (should be none in this case)
        let tokens: Vec<Token> = poller.iter().collect();
        assert!(tokens.is_empty());
    }

    #[test]
    fn lagging_subscriber_should_not_emit_more_items_than_queue_can_retain() {
        let topic = Arc::new(Topic::new("test_lagging_subscriber".to_string(), 2, 8));
        let publisher = topic.create_publisher();
        let mut subscriber = topic.create_subscriber();

        for value in 0_u32..4 {
            publisher.publish(value);
        }

        let mut received = Vec::new();
        while subscriber.check_update() {
            received.push(subscriber.check_update_and_copy().unwrap());
        }

        assert_eq!(
            received,
            vec![2, 3],
            "a subscriber that falls behind the ring buffer should only observe the retained tail"
        );
    }

    #[test]
    fn concurrent_publishers_should_not_drop_or_duplicate_messages() {
        const PRODUCERS: usize = 8;
        const MESSAGES_PER_PRODUCER: usize = 250;
        let topic = Arc::new(Topic::new(
            "test_concurrent_publishers".to_string(),
            (PRODUCERS * MESSAGES_PER_PRODUCER) as u16,
            9,
        ));
        let barrier = Arc::new(Barrier::new(PRODUCERS));

        let mut handles = Vec::new();
        for producer_id in 0..PRODUCERS {
            let topic = topic.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                let publisher = topic.create_publisher();
                barrier.wait();
                for seq in 0..MESSAGES_PER_PRODUCER {
                    publisher.publish((producer_id * MESSAGES_PER_PRODUCER + seq) as u32);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut subscriber = topic.create_subscriber();
        let mut received = Vec::new();
        while subscriber.check_update() {
            received.push(subscriber.check_update_and_copy().unwrap());
        }

        let unique: HashSet<_> = received.iter().copied().collect();
        assert_eq!(
            received.len(),
            PRODUCERS * MESSAGES_PER_PRODUCER,
            "every published message should be readable exactly once"
        );
        assert_eq!(
            unique.len(),
            PRODUCERS * MESSAGES_PER_PRODUCER,
            "concurrent publishers produced duplicate or lost messages"
        );
    }

    #[test]
    fn subscriber_read_after_new_generation_observes_committed_data() {
        let topic = Arc::new(Topic::new("test_generation_visibility".to_string(), 4, 10));
        let writer_has_published_generation = Arc::new(AtomicBool::new(false));
        let writer_may_unlock_fifo = Arc::new(Barrier::new(2));

        let topic_for_writer = topic.clone();
        let published_flag = writer_has_published_generation.clone();
        let unlock_barrier = writer_may_unlock_fifo.clone();
        let handle = thread::spawn(move || {
            let mut fifo = topic_for_writer.fifo.lock().unwrap();
            fifo[0] = Some(123_u32);
            topic_for_writer.generation.store(1, Ordering::Release);
            published_flag.store(true, Ordering::Release);

            // Keep the fifo lock held after publishing generation to model the
            // critical window where a subscriber can observe the update before
            // it can enter the queue.
            unlock_barrier.wait();
        });

        while !writer_has_published_generation.load(Ordering::Acquire) {
            thread::yield_now();
        }

        let mut subscriber = topic.create_subscriber();
        assert!(subscriber.check_update());

        writer_may_unlock_fifo.wait();
        let data = subscriber.check_update_and_copy();

        assert_eq!(data, Some(123));
        handle.join().unwrap();
    }
}
