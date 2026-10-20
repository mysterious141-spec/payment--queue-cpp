# Payment Processing Queue (C++)

## Question
Describe real-world applications that require queues as a data structure, explain why queues are necessary, and choose one application to develop.

---

## 1. Real-world applications of queues

- **Customer support / call centers:** callers are served in arrival order (FIFO), so it's fair.
- **Print spooling:** the printer is slow, so jobs wait their turn instead of blocking apps.
- **CPU / OS task scheduling:** processes wait in ready queues for CPU time.
- **Message brokers (Kafka, RabbitMQ, SQS):** services hand off work without waiting on each other.
- **Payment / transaction processing:** requests arrive in bursts and must be handled in order.
- **Web server request handling:** incoming requests are buffered when traffic exceeds capacity.
- **BFS / network packet routing:** nodes and packets are processed in arrival order.

## 2. Why queues are necessary

1. **Ordering and fairness:** first in, first out guarantees nobody gets skipped.
2. **Decoupling:** producers don't wait for slow consumers.
3. **Buffering bursts:** traffic spikes get absorbed instead of crashing the system.
4. **Backpressure and reliability:** a bounded queue limits load, and unprocessed work isn't lost.

## 3. Chosen application: payment processing queue

Payments must be processed in order per account (a withdrawal can't jump ahead of the deposit that funds it), and spikes shouldn't overload the core system. A producer-consumer design fits:

- **Producer:** the API layer calls `submit()` to add a payment.
- **Queue:** a bounded FIFO queue buffers payments. `submit()` returns `false` when full (backpressure).
- **Consumer:** worker threads take payments and process them.

### Type of queue
A bounded, thread-safe, blocking FIFO queue: `std::queue` wrapped in a `PaymentQueue` class with a `std::mutex` and `std::condition_variable`.

### Design notes
- **Ordering per account:** one queue per worker, routed by `hash(account) % N`. Different accounts run in parallel, while each account stays strictly ordered.
- **Workers sleep when idle:** `condition_variable` avoids busy waiting.
- **Failures:** add a retry queue and a dead-letter queue.
- **Production:** swap the in-memory queue for Kafka/RabbitMQ so payments survive a crash.

### Complexity
Enqueue and dequeue are both O(1).

## 4. Code

See [`payments.cpp`](payments.cpp).

## 5. Build and run

Requires a C++17 compiler (g++ / MinGW).

```
g++ -std=c++17 -pthread payments.cpp -o payments
./payments        # Windows PowerShell: .\payments
```

## 6. Sample output

```
[worker 0] processed P2 for ACC-200 ($75.5)
[worker 1] processed P4 for ACC-300 ($900)
[worker 2] processed P1 for ACC-100 ($250)
[worker 2] processed P3 for ACC-100 ($-100)
[worker 0] processed P5 for ACC-200 ($-25)
```

The same account always lands on the same worker, in order (P1 then P3, P2 then P5). Order between different accounts can vary from run to run.