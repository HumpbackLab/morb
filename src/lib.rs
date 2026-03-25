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
        self.topics.insert(name, Box::new(topic.clone()));
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
        let index = (self.topic.generation.load(Ordering::SeqCst) as usize)
            % (self.topic.queue_size as usize);
        self.topic.fifo.lock().unwrap()[index] = Some(data);
        self.topic.generation.fetch_add(1, Ordering::SeqCst);
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
        if self.sub_generation != topic_generation {
            let index = (self.sub_generation as usize) % (self.topic.queue_size as usize);
            self.sub_generation += 1;
            return self.topic.fifo.lock().unwrap()[index].clone();
        }
        None
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
}
