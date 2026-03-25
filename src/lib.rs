use std::sync::{RwLock,Arc,Mutex,LazyLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};


pub trait MorbDataType: Send + Sync + 'static + Clone{}
impl<T> MorbDataType for T where T: Send + Sync + 'static + Clone{}

pub struct TopicManager {
    topics: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    topic_num:usize
}

static TOPIC_MANAGER:LazyLock<Arc<RwLock<TopicManager>>> = LazyLock::new(||{
    Arc::new(RwLock::new(TopicManager::new()))
});

pub fn create_topic<T:MorbDataType>(name: String, queue_size:u16) -> Arc<Topic<T>> {
    TOPIC_MANAGER.write().unwrap().create_topic(name, queue_size)
}

pub fn get_topic<T:MorbDataType>(name: &str) -> Option<Arc<Topic<T>>> {
    TOPIC_MANAGER.read().unwrap().get_topic(name)
}


impl TopicManager {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            topic_num:0
        }
    }

    pub fn create_topic<T:MorbDataType>(&mut self, name: String, queue_size:u16) -> Arc<Topic<T>> {
        self.topic_num += 1;
        let topic = Arc::new(Topic::new(name.clone(), queue_size, self.topic_num));
        // use topic_num as topic_id, start from 1
        
        self.topics.insert(name, Box::new(topic.clone()));
        topic
    }

    pub fn get_topic<T:MorbDataType>(&self, name: &str) -> Option<Arc<Topic<T>>> {
        self.topics.get(name).and_then(|boxed| boxed.downcast_ref::<Arc<Topic<T>>>().cloned())
    }
}


pub struct Publisher<T:MorbDataType> {
    topic: Arc<Topic<T>>,
}

impl <T:MorbDataType> Publisher<T> {
    pub fn publish(&self, data: T) {
        let index = (self.topic.generation.load(Ordering::SeqCst) as usize) % (self.topic.queue_size as usize);
        self.topic.fifo.lock().unwrap() [index] = Some(data);
        self.topic.generation.fetch_add(1, Ordering::SeqCst);
        // Notify subscribers in wait_list
        // while let Some(subscriber_id) = self.topic.wait_list.pop() {
        //     // Here you would notify the subscriber with the new data
        //     // This is a placeholder for the actual notification logic
        //     println!("Notifying subscriber {} of new data", subscriber_id);
        // }
    }
}

pub struct Subscriber<T:MorbDataType> {
    topic: Arc<Topic<T>>,
    sub_generation: u32,
}

impl <T:MorbDataType> Subscriber<T> {
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

pub struct Topic<T:MorbDataType> {
    name: String,
    fifo: Mutex<Vec<Option<T>>>,
    pub(crate) generation:AtomicU32,
    wait_list: Vec<u64>,
    queue_size:u16,
    topic_id:usize
}

impl<T: MorbDataType> Topic<T> {
    // use create_topic, this is private
    fn new(name: String, queue_size:u16, topic_id:usize) -> Self {
        Self {
            name,
            fifo: Mutex::new(vec![None; queue_size as usize]),
            generation: AtomicU32::new(0),
            wait_list:Vec::new(),
            queue_size,
            topic_id
        }
    }

    pub fn create_publisher(self:&Arc<Self>) -> Publisher<T> {
        Publisher {
            topic: self.clone(),
        }
    }

    pub fn create_subscriber(self:&Arc<Self>) -> Subscriber<T> {
        Subscriber {
            topic: self.clone(),
            sub_generation: 0,
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct test_struct {
        test:u32,
        test2:u64
    }
    #[test]
    fn basic_topic_pub_sub_test() {
        let test_topic = create_topic::<test_struct>("test_struct_basic".to_string(), 16);
        let publisher_struct = test_topic.create_publisher();
        publisher_struct.publish(test_struct { test: 1, test2: 2 });
        assert_eq!(test_topic.generation.load(Ordering::SeqCst), 1);


        let publisher = test_topic.create_publisher();
        publisher.publish(test_struct { test: 42, test2: 43 });
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
        let topic:Topic<u16> = Topic::new("test".to_string(),16, 1);
        assert_eq!(topic.name, "test");
        assert_eq!(topic.generation.load(Ordering::SeqCst), 0);
    }
}
