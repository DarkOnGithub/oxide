use std::sync::Arc;

use oxide_core::WorkStealingQueue;

#[test]
fn global_submission_can_be_stolen() {
    let queue = Arc::new(WorkStealingQueue::new(2));
    let worker = queue.worker(0).expect("worker 0 should exist");

    queue.submit(42usize);

    assert_eq!(worker.steal(), Some(42));
    assert!(queue.is_empty());
}

#[test]
fn local_queue_is_lifo() {
    let queue = Arc::new(WorkStealingQueue::new(1));
    let worker = queue.worker(0).expect("worker 0 should exist");

    worker.push_local(1usize);
    worker.push_local(2usize);
    worker.push_local(3usize);

    assert_eq!(worker.steal(), Some(3));
    assert_eq!(worker.steal(), Some(2));
    assert_eq!(worker.steal(), Some(1));
    assert_eq!(worker.steal(), None);
    assert!(queue.is_empty());
}

#[test]
fn worker_can_steal_from_peer_local_queue() {
    let queue = Arc::new(WorkStealingQueue::new(2));
    let worker0 = queue.worker(0).expect("worker 0 should exist");
    let worker1 = queue.worker(1).expect("worker 1 should exist");

    worker0.push_local(99usize);

    assert_eq!(worker1.steal(), Some(99));
    assert!(queue.is_empty());
}

#[test]
fn worker_slot_is_single_consumer() {
    let queue = Arc::new(WorkStealingQueue::<usize>::new(1));

    let first = queue.worker(0);
    let second = queue.worker(0);

    assert!(first.is_some());
    assert!(second.is_none());
}
