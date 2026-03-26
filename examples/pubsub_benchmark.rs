use morb::{TopicPoller, create_topic};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

static TOPIC_ID: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)]
#[derive(Clone, Copy)]
struct Payload64 {
    bytes: [u8; 64],
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
struct Payload256 {
    bytes: [u8; 256],
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
struct Payload1024 {
    bytes: [u8; 1024],
}

fn unique_topic_name(prefix: &str) -> String {
    let id = TOPIC_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

fn fmt_ns_per_op(duration: Duration, ops: u64) -> f64 {
    duration.as_secs_f64() * 1_000_000_000.0 / ops as f64
}

fn fmt_ops_per_sec(duration: Duration, ops: u64) -> f64 {
    ops as f64 / duration.as_secs_f64()
}

fn print_result(name: &str, ops: u64, elapsed: Duration) {
    println!(
        "{name:<18} {ops:>12} ops  {:>12.2} ns/op  {:>12.2} ops/s",
        fmt_ns_per_op(elapsed, ops),
        fmt_ops_per_sec(elapsed, ops),
    );
}

fn benchmark_publish_only(iterations: u64, queue_size: u16) {
    let topic = create_topic::<u64>(unique_topic_name("publish_only"), queue_size).unwrap();
    let publisher = topic.create_publisher();

    let started = Instant::now();
    for value in 0..iterations {
        publisher.publish(std::hint::black_box(value));
    }
    print_result("publish_only", iterations, started.elapsed());
}

fn benchmark_publish_consume(iterations: u64, queue_size: u16) {
    let topic = create_topic::<u64>(unique_topic_name("publish_consume"), queue_size).unwrap();
    let publisher = topic.create_publisher();
    let mut subscriber = topic.create_subscriber();

    let started = Instant::now();
    for value in 0..iterations {
        publisher.publish(std::hint::black_box(value));
        std::hint::black_box(subscriber.check_update_and_copy().unwrap());
    }
    print_result("publish_consume", iterations, started.elapsed());
}

fn benchmark_publish_poll(iterations: u64, queue_size: u16) {
    let topic = create_topic::<u64>(unique_topic_name("publish_poll"), queue_size).unwrap();
    let publisher = topic.create_publisher();
    let mut poller = TopicPoller::new();
    poller.add_topic(&topic).unwrap();

    let started = Instant::now();
    for value in 0..iterations {
        publisher.publish(std::hint::black_box(value));
        poller.wait(Some(Duration::ZERO)).unwrap();
        for token in poller.iter() {
            std::hint::black_box(token);
        }
    }
    print_result("publish_poll", iterations, started.elapsed());
}

fn benchmark_multi_producer(iterations: u64) {
    const PRODUCERS: usize = 4;

    let topic = create_topic::<u64>(unique_topic_name("multi_producer"), 1024).unwrap();
    let barrier = Arc::new(Barrier::new(PRODUCERS + 1));
    let per_producer = iterations / PRODUCERS as u64;
    let total_ops = per_producer * PRODUCERS as u64;

    let mut handles = Vec::new();
    for producer_id in 0..PRODUCERS {
        let barrier = barrier.clone();
        let publisher = topic.create_publisher();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for seq in 0..per_producer {
                publisher.publish((producer_id as u64 * per_producer) + seq);
            }
        }));
    }

    let started = Instant::now();
    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }

    print_result("multi_producer", total_ops, started.elapsed());
}

fn benchmark_multi_subscriber(iterations: u64) {
    const SUBSCRIBERS: usize = 4;

    let messages = iterations.min(50_000);
    let queue_size = messages as u16;
    let topic = create_topic::<u64>(unique_topic_name("multi_subscriber"), queue_size).unwrap();
    let publisher = topic.create_publisher();
    let start_barrier = Arc::new(Barrier::new(SUBSCRIBERS + 1));

    let mut handles = Vec::new();
    for _ in 0..SUBSCRIBERS {
        let start_barrier = start_barrier.clone();
        let mut subscriber = topic.create_subscriber();
        handles.push(thread::spawn(move || {
            start_barrier.wait();
            for _ in 0..messages {
                loop {
                    if let Some(value) = subscriber.check_update_and_copy() {
                        std::hint::black_box(value);
                        break;
                    }
                    thread::yield_now();
                }
            }
        }));
    }

    let started = Instant::now();
    start_barrier.wait();
    for value in 0..messages {
        publisher.publish(std::hint::black_box(value));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    print_result(
        "multi_subscriber",
        messages * SUBSCRIBERS as u64,
        started.elapsed(),
    );
}

fn benchmark_large_message<T>(name: &str, iterations: u64, sample: T)
where
    T: Clone + Copy + Send + Sync + 'static,
{
    let topic = create_topic::<T>(unique_topic_name(name), 1024).unwrap();
    let publisher = topic.create_publisher();
    let mut subscriber = topic.create_subscriber();

    let started = Instant::now();
    for _ in 0..iterations {
        publisher.publish(std::hint::black_box(sample));
        std::hint::black_box(subscriber.check_update_and_copy().unwrap());
    }

    print_result(name, iterations, started.elapsed());
}

fn benchmark_blocking_poll(iterations: u64) {
    let waits = iterations.min(50_000);
    let topic = create_topic::<u64>(unique_topic_name("blocking_poll"), 1024).unwrap();
    let publisher = topic.create_publisher();
    let (tx, rx) = mpsc::sync_channel::<()>(0);
    let topic_for_thread = topic.clone();

    let handle = thread::spawn(move || {
        for value in 0..waits {
            rx.recv().unwrap();
            publisher.publish(value);
        }
        drop(topic_for_thread);
    });

    let mut poller = TopicPoller::new();
    poller.add_topic(&topic).unwrap();

    let started = Instant::now();
    for _ in 0..waits {
        tx.send(()).unwrap();
        poller.wait(Some(Duration::from_secs(1))).unwrap();
        for token in poller.iter() {
            std::hint::black_box(token);
        }
        topic.clear_event();
    }
    handle.join().unwrap();

    print_result("blocking_poll", waits, started.elapsed());
}

fn parse_iterations() -> u64 {
    env::args()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1_000_000)
}

fn main() {
    let iterations = parse_iterations();
    let queue_size = 1024;

    println!("morb benchmark");
    println!("iterations: {iterations}");
    println!("queue_size: {queue_size}");

    benchmark_publish_only(iterations, queue_size);
    benchmark_publish_consume(iterations, queue_size);
    benchmark_publish_poll(iterations, queue_size);
    benchmark_multi_producer(iterations);
    benchmark_multi_subscriber(iterations);
    benchmark_large_message("large_msg_64", iterations, Payload64 { bytes: [7; 64] });
    benchmark_large_message("large_msg_256", iterations, Payload256 { bytes: [9; 256] });
    benchmark_large_message(
        "large_msg_1024",
        iterations,
        Payload1024 { bytes: [11; 1024] },
    );
    benchmark_blocking_poll(iterations);
}
