use morb::{TopicPoller, create_topic};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static TOPIC_ID: AtomicU64 = AtomicU64::new(0);

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

fn benchmark_publish_only(iterations: u64, queue_size: u16) {
    let topic = create_topic::<u64>(unique_topic_name("publish_only"), queue_size).unwrap();
    let publisher = topic.create_publisher();

    let started = Instant::now();
    for value in 0..iterations {
        publisher.publish(std::hint::black_box(value));
    }
    let elapsed = started.elapsed();

    println!(
        "publish_only      {:>12} ops  {:>12.2} ns/op  {:>12.2} ops/s",
        iterations,
        fmt_ns_per_op(elapsed, iterations),
        fmt_ops_per_sec(elapsed, iterations),
    );
}

fn benchmark_publish_and_consume(iterations: u64, queue_size: u16) {
    let topic = create_topic::<u64>(unique_topic_name("publish_consume"), queue_size).unwrap();
    let publisher = topic.create_publisher();
    let mut subscriber = topic.create_subscriber();

    let started = Instant::now();
    for value in 0..iterations {
        publisher.publish(std::hint::black_box(value));
        let received = subscriber.check_update_and_copy().unwrap();
        std::hint::black_box(received);
    }
    let elapsed = started.elapsed();

    println!(
        "publish_consume   {:>12} ops  {:>12.2} ns/op  {:>12.2} ops/s",
        iterations,
        fmt_ns_per_op(elapsed, iterations),
        fmt_ops_per_sec(elapsed, iterations),
    );
}

fn benchmark_publish_and_poll(iterations: u64, queue_size: u16) {
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
    let elapsed = started.elapsed();

    println!(
        "publish_poll      {:>12} ops  {:>12.2} ns/op  {:>12.2} ops/s",
        iterations,
        fmt_ns_per_op(elapsed, iterations),
        fmt_ops_per_sec(elapsed, iterations),
    );
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
    benchmark_publish_and_consume(iterations, queue_size);
    benchmark_publish_and_poll(iterations, queue_size);
}
