#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

struct Payment {
    std::string id;
    std::string account;
    double amount;
};

// Bounded, thread-safe queue
class PaymentQueue {
    std::queue<Payment> q_;
    std::mutex m_;
    std::condition_variable notEmpty_;
    size_t capacity_;
    bool closed_ = false;

public:
    explicit PaymentQueue(size_t cap) : capacity_(cap) {}

    // Producer: returns false if full or closed (backpressure)
    bool submit(Payment p) {
        std::lock_guard<std::mutex> lock(m_);
        if (closed_ || q_.size() >= capacity_) return false;
        q_.push(std::move(p));
        notEmpty_.notify_one();
        return true;
    }

    // Consumer: blocks until work arrives; false when closed and drained
    bool take(Payment& out) {
        std::unique_lock<std::mutex> lock(m_);
        notEmpty_.wait(lock, [&] { return !q_.empty() || closed_; });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop();
        return true;
    }

    void close() {
        { std::lock_guard<std::mutex> lock(m_); closed_ = true; }
        notEmpty_.notify_all();
    }
};

class PaymentProcessor {
    std::vector<PaymentQueue*> queues_;
    std::vector<std::thread> workers_;
    std::mutex printMutex_;

public:
    explicit PaymentProcessor(size_t numWorkers, size_t capacity = 1000) {
        for (size_t i = 0; i < numWorkers; ++i)
            queues_.push_back(new PaymentQueue(capacity));
        for (size_t i = 0; i < numWorkers; ++i)
            workers_.emplace_back([this, i] {
                Payment p;
                while (queues_[i]->take(p)) process(p, i);
            });
    }

    // Same account always hits the same queue => strict per-account order
    bool submit(const Payment& p) {
        size_t idx = std::hash<std::string>{}(p.account) % queues_.size();
        return queues_[idx]->submit(p);
    }

    void shutdown() {
        for (auto* q : queues_) q->close();
        for (auto& t : workers_) t.join();
        for (auto* q : queues_) delete q;
        queues_.clear();
    }

private:
    void process(const Payment& p, size_t worker) {
        std::lock_guard<std::mutex> lock(printMutex_);
        std::cout << "[worker " << worker << "] processed " << p.id
                  << " for " << p.account << " ($" << p.amount << ")\n";
    }
};

int main() {
    PaymentProcessor processor(3);

    processor.submit({"P1", "ACC-100", 250.00});
    processor.submit({"P2", "ACC-200", 75.50});
    processor.submit({"P3", "ACC-100", -100.00});
    processor.submit({"P4", "ACC-300", 900.00});
    processor.submit({"P5", "ACC-200", -25.00});

    processor.shutdown();
}