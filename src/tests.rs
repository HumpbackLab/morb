use super::*;
use std::collections::HashSet;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::thread;

#[derive(Clone, Debug, PartialEq)]
struct TestStruct {
    test: u32,
    test2: u64,
}

#[test]
fn basic_topic_pub_sub_test() {
    let test_topic = create_topic::<TestStruct>("test_struct_basic_poll".to_string(), 16).unwrap();
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
    assert_eq!(topic.name(), "test");
    assert_eq!(topic.generation.load(Ordering::SeqCst), 0);
    assert_eq!(topic.token(), Token(1));
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
        assert_eq!(i, topic.token());
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
fn topic_poller_zero_timeout_without_updates_returns_no_tokens() {
    let mut poller = TopicPoller::new();
    let topic: Topic<u32> = Topic::new("test_zero_timeout_empty".to_string(), 16, 11);

    poller.add_topic(&topic).unwrap();
    poller.wait(Some(Duration::ZERO)).unwrap();

    let tokens: Vec<Token> = poller.iter().collect();
    assert!(tokens.is_empty());
}

#[test]
fn topic_poller_zero_timeout_observes_updates_without_clearing_eventfd() {
    let mut poller = TopicPoller::new();
    let topic = Arc::new(Topic::new("test_zero_timeout_update".to_string(), 16, 12));
    let publisher = topic.create_publisher();

    poller.add_topic(&topic).unwrap();

    publisher.publish(7_u32);
    poller.wait(Some(Duration::ZERO)).unwrap();
    let tokens: Vec<Token> = poller.iter().collect();
    assert_eq!(tokens, vec![topic.token()]);

    publisher.publish(8_u32);
    poller.wait(Some(Duration::ZERO)).unwrap();
    let tokens: Vec<Token> = poller.iter().collect();
    assert_eq!(tokens, vec![topic.token()]);
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
fn subscriber_read_blocking_returns_immediately_when_data_is_available() {
    let topic = Arc::new(Topic::new("test_read_blocking_ready".to_string(), 4, 13));
    let publisher = topic.create_publisher();
    let mut subscriber = topic.create_subscriber();

    publisher.publish(42_u32);

    assert_eq!(subscriber.read_blocking(), 42);
}

#[test]
fn subscriber_read_blocking_waits_until_publish_arrives() {
    let topic = Arc::new(Topic::new("test_read_blocking_wait".to_string(), 4, 14));
    let start_barrier = Arc::new(Barrier::new(2));

    let topic_for_reader = topic.clone();
    let start_barrier_for_reader = start_barrier.clone();
    let handle = thread::spawn(move || {
        let mut subscriber = topic_for_reader.create_subscriber();
        start_barrier_for_reader.wait();
        subscriber.read_blocking()
    });

    start_barrier.wait();
    thread::sleep(Duration::from_millis(20));
    topic.create_publisher().publish(77_u32);

    assert_eq!(handle.join().unwrap(), 77);
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
        if let Some(data) = subscriber.check_update_and_copy() {
            received.push(data);
        }
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
        fifo.write(0, 123_u32);
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

#[test]
fn create_topic_with_existing_name_returns_error() {
    let topic_name = "test_duplicate_topic".to_string();

    // First creation should succeed
    let result1 = create_topic::<u32>(topic_name.clone(), 16);
    assert!(result1.is_ok());

    // Second creation with same name should fail
    let result2 = create_topic::<u32>(topic_name, 16);
    assert!(result2.is_err());
}

#[test]
fn get_or_create_topic_creates_missing_topic() {
    let topic_name = "test_get_or_create_new_topic".to_string();

    let topic = get_or_create_topic::<u32>(topic_name.clone(), 16).unwrap();
    assert_eq!(topic.name(), topic_name);

    let publisher = topic.create_publisher();
    publisher.publish(99);

    let fetched = get_topic::<u32>("test_get_or_create_new_topic").unwrap();
    let mut subscriber = fetched.create_subscriber();
    assert_eq!(subscriber.check_update_and_copy(), Some(99));
}

#[test]
fn get_or_create_topic_returns_existing_topic() {
    let topic_name = "test_get_or_create_existing_topic".to_string();

    let topic = create_topic::<u32>(topic_name.clone(), 16).unwrap();
    let token = topic.token();
    topic.create_publisher().publish(123);

    let same_topic = get_or_create_topic::<u32>(topic_name, 32).unwrap();
    assert_eq!(same_topic.token(), token);

    let mut subscriber = same_topic.create_subscriber();
    assert_eq!(subscriber.check_update_and_copy(), Some(123));
}

#[test]
fn duplicate_topic_creation_does_not_replace_existing_topic() {
    let topic_name = "test_no_replace_topic".to_string();

    // Create first topic and hold a reference
    let topic1 = create_topic::<u32>(topic_name.clone(), 16).unwrap();
    let publisher1 = topic1.create_publisher();
    publisher1.publish(123);

    // Try to create duplicate - should fail
    let result2 = create_topic::<u32>(topic_name.clone(), 16);
    assert!(result2.is_err());

    // Verify get_topic returns the original topic
    let topic_from_get = get_topic::<u32>(&topic_name).unwrap();

    // Verify we can still read from the original topic
    let mut subscriber = topic1.create_subscriber();
    assert_eq!(subscriber.check_update_and_copy(), Some(123));

    // Verify the topic from get_topic is the same instance
    // (by publishing and reading from it)
    let publisher_from_get = topic_from_get.create_publisher();
    publisher_from_get.publish(456);

    let mut subscriber2 = topic_from_get.create_subscriber();
    assert_eq!(subscriber2.check_update_and_copy(), Some(123));
    assert_eq!(subscriber2.check_update_and_copy(), Some(456));
}

#[test]
fn topic_num_not_incremented_on_duplicate() {
    let topic_name1 = "test_topic_num_1".to_string();
    let topic_name2 = "test_topic_num_2".to_string();
    let topic_name3 = "test_topic_num_3".to_string();

    // Create first topic
    let topic1 = create_topic::<u32>(topic_name1.clone(), 16).unwrap();
    let token1 = topic1.token();

    // Try to create duplicate of first topic (should not increment topic_num)
    let _ = create_topic::<u32>(topic_name1, 16);

    // Create second topic - its token should be token1 + 1, not +2
    let topic2 = create_topic::<u32>(topic_name2, 16).unwrap();
    let token2 = topic2.token();
    assert_eq!(usize::from(token2), usize::from(token1) + 1);

    // Try to create duplicate of second topic
    let _ = create_topic::<u32>(topic_name3.clone(), 16).unwrap();
    let _ = create_topic::<u32>(topic_name3, 16);

    // Create third topic - should increment correctly
    let topic3 = create_topic::<u32>("test_topic_num_final".to_string(), 16).unwrap();
    assert_eq!(usize::from(topic3.token()), usize::from(token2) + 2);
}
