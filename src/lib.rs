use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};


pub trait MorbDataType: Send + Sync + 'static + Clone{}
impl<T> MorbDataType for T where T: Send + Sync + 'static + Clone{}

pub struct TopicManager {
    topics: Arc<RwLock<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>>,
}

impl TopicManager {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_topic<T:MorbDataType>(&self, name: String, queue_size:u16) -> Arc<Topic<T>> {
        let topic = Arc::new(Topic::new(name.clone(), queue_size));
        self.topics.write().unwrap().insert(name, Box::new(topic.clone()));
        topic
    }

    pub fn get_topic<T:MorbDataType>(&self, name: &str) -> Option<Arc<Topic<T>>> {
        self.topics.read().unwrap().get(name).and_then(|boxed| boxed.downcast_ref::<Arc<Topic<T>>>().cloned())
    }
}


struct Publisher<T:MorbDataType> {
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

struct Subscriber<T:MorbDataType> {
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

struct Topic<T:MorbDataType> {
    name: String,
    fifo: Mutex<Vec<Option<T>>>,
    generation:AtomicU32,
    wait_list: Vec<u64>,
    queue_size:u16,
}

impl<T: MorbDataType> Topic<T> {
    // use create_topic, this is private
    fn new(name: String, queue_size:u16) -> Self {
        Self {
            name,
            fifo: Mutex::new(vec![None; queue_size as usize]),
            generation: AtomicU32::new(0),
            wait_list:Vec::new(),
            queue_size,
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
        let manager = TopicManager::new();
        let test_topic = manager.create_topic::<test_struct>("test_struct".to_string(), 16);
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
        let topic:Topic<u16> = Topic::new("test".to_string(),16);
        assert_eq!(topic.name, "test");
        assert_eq!(topic.generation.load(Ordering::SeqCst), 0);
    }
}
