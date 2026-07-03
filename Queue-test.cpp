/*
 * Copyright 2026 L. Richard Moore Jr.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <future>
#include <memory>
#include <vector>
#include "Queue.hpp"

using namespace thr;

TEST_CASE( "Create / destroy Queue" ) {
	Queue<int>* queue1 = new Queue<int>([](int datum){
	});

	delete queue1;
}

TEST_CASE( "Disconnected queue" ) {
	int count = 0;
	Queue<int>* queue1 = new Queue<int>([&count](int datum){
		count++;
	});

	queue1->push(10);
	std::this_thread::sleep_for (std::chrono::milliseconds(100));
	delete queue1;

	CHECK(count == 1);
}

TEST_CASE( "Overflow" ) {
	int count = 0;
	Queue<int> queue1([&count](int datum){
		std::this_thread::sleep_for (std::chrono::milliseconds(100));
		count += datum;
	});

	queue1.push(1);
	queue1.push(2);
	queue1.push(4);

	std::this_thread::sleep_for (std::chrono::milliseconds(200));

	bool countIsSingular = count == 4 || count == 2 || count == 1;
	CHECK(countIsSingular);
	CHECK(queue1.overflowCounter() == 2);
}

TEST_CASE( "Overflow - save newest" ) {
	int count = 0;
	Queue<int> queue1([&count](int datum){
		std::this_thread::sleep_for (std::chrono::milliseconds(100));
		count = datum;
	});

	queue1.push(1);
	queue1.push(2);
	queue1.push(4);

	std::this_thread::sleep_for (std::chrono::milliseconds(200));

	CHECK(count == 4);
	CHECK(queue1.overflowCounter() == 2);
}

TEST_CASE( "No Overflow" ) {
	int count = 0;
	Queue<int> queue1([&count](int datum){
		std::this_thread::sleep_for (std::chrono::milliseconds(10));
		count += datum;
	});

	// The worker requires 10ms for each work item, so give it a little longer between
	// submissions.  Thus, it should never overflow.
	queue1.push(1);
	std::this_thread::sleep_for (std::chrono::milliseconds(11));
	queue1.push(2);
	std::this_thread::sleep_for (std::chrono::milliseconds(11));
	queue1.push(4);
	std::this_thread::sleep_for (std::chrono::milliseconds(11));

	// Waiting 11 ms isnt' always enough, so we wait() to be sure.
	queue1.wait();

	CHECK(count == 7);
	CHECK(queue1.overflowCounter() == 0);
}

TEST_CASE( "Long queue" ) {
	int sum = 0;
	Queue<int> queue1([&sum](int datum){
		static bool firstTimeThrough = true;
		if(firstTimeThrough) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			firstTimeThrough = false;
		}

		sum += datum;
	}, 0);

	// The worker requires 10ms for each work item, so give it a little longer between
	// submissions.  Thus, it should never overflow.
	queue1.push(1);
	queue1.push(2);
	queue1.push(4);
	queue1.push(1);
	queue1.push(1);
	queue1.push(1);
	queue1.push(1);
	queue1.push(1);
	queue1.push(1);

	const size_t queuedWork = queue1.queuedWork();
	// Depends on timing betweeen threads
	CHECK(((queuedWork == 9) || (queuedWork == 8)));

	// Waiting 11 ms isnt' always enough, so we wait() to be sure.
	queue1.wait();

	CHECK(sum == 13);
	CHECK(queue1.overflowCounter() == 0);
}

TEST_CASE( "Complicated create / destroy Queue" ) {
	for(int index = 0; index < 100; ++index) {
		Queue<int>* queue1 = new Queue<int>([](int datum){
			std::this_thread::sleep_for(std::chrono::milliseconds(50));

		});

		queue1->push(1);
		delete queue1;

	}
}

TEST_CASE( "wait() must not hang when destroyed while worker is mid-callback" ) {
	// Regression test: if terminate becomes true (via the destructor) while the worker
	// thread is off executing workFunction (queue already empty, idle already false),
	// the worker must still release any thread concurrently blocked in wait() before
	// the worker thread (and the Queue's synchronization primitives) go away.
	Queue<int>* queue1 = new Queue<int>([](int datum){
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	});

	queue1->push(1);
	// Give the worker time to pop the item (queue empty) and start the 200ms callback
	// (idle == false), landing in the window the bug depends on.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));

	std::promise<void> waitReturned;
	std::future<void> waitFuture = waitReturned.get_future();
	std::thread waiter([queue1, &waitReturned]{
		queue1->wait();
		waitReturned.set_value();
	});

	// Give the waiter a moment to actually enter the blocking wait().
	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	delete queue1;

	// A correct implementation releases any concurrent wait() callers once destruction
	// completes. If it doesn't, the future never becomes ready.
	bool released = waitFuture.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
	CHECK(released);

	if(released) {
		waiter.join();
	} else {
		// The waiter is permanently stuck on now-destroyed synchronization primitives;
		// detach so the test process can still exit instead of hanging here forever.
		waiter.detach();
	}
}

TEST_CASE( "wait() returns immediately when nothing has ever been pushed" ) {
	Queue<int> queue1([](int datum){});

	auto waiter = std::async(std::launch::async, [&queue1]{ queue1.wait(); });
	CHECK(waiter.wait_for(std::chrono::milliseconds(200)) == std::future_status::ready);
}

TEST_CASE( "Queue with no work function silently drops items" ) {
	// The workFunction defaults to nullptr; pushing/draining must not crash.
	Queue<int> queue1;

	queue1.push(1);
	queue1.push(2);
	queue1.wait();

	CHECK(queue1.queueCounter() == 2);
}

TEST_CASE( "isFull() is always false for an unlimited queue" ) {
	// Regression for a bug where isFull() ignored the "0 means unlimited" contract
	// that push() itself honors, so it always reported full for limit == 0.
	std::promise<void> release;
	std::shared_future<void> releaseFuture = release.get_future();
	Queue<int> queue1([releaseFuture](int datum){ releaseFuture.wait(); }, 0);

	for(int i = 0; i < 10; ++i) queue1.push(i);
	// Let the worker pick up the first item so the other 9 pile up in the deque.
	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	CHECK(queue1.queuedWork() == 9);
	CHECK_FALSE(queue1.isFull());

	release.set_value();
	queue1.wait();
	CHECK_FALSE(queue1.isFull());
}

TEST_CASE( "isFull()/hasWork()/queuedWork() reflect queued items, not in-flight work" ) {
	std::promise<void> release;
	std::shared_future<void> releaseFuture = release.get_future();
	Queue<int> queue1([releaseFuture](int datum){ releaseFuture.wait(); }, 2);

	queue1.push(1);
	// Let the worker grab item 1 (queue empties, worker blocks in the callback on
	// releaseFuture) so this is genuinely "1 in flight, 0 queued", not a race.
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	CHECK_FALSE(queue1.hasWork());
	CHECK_FALSE(queue1.isFull());
	CHECK_FALSE(queue1.isIdle());

	queue1.push(2);
	queue1.push(3);
	CHECK(queue1.hasWork());
	CHECK(queue1.queuedWork() == 2);
	CHECK(queue1.isFull());

	release.set_value();
	queue1.wait();

	CHECK_FALSE(queue1.hasWork());
	CHECK_FALSE(queue1.isFull());
	CHECK(queue1.isIdle());
}

TEST_CASE( "queueCounter counts and resets independently of overflowCounter" ) {
	Queue<int> queue1([](int datum){
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}, 1);

	queue1.push(1);
	queue1.push(2);
	queue1.push(3);

	// All three pushes count, regardless of whether they overflowed.
	CHECK(queue1.queueCounter() == 3);
	// A second read without any intervening push must come back to zero.
	CHECK(queue1.queueCounter() == 0);

	queue1.wait();
	queue1.push(4);
	CHECK(queue1.queueCounter() == 1);
}

TEST_CASE( "Overflow with a limit greater than one" ) {
	// This used to push all 5 items in a tight loop with no synchronization, and assert a
	// specific overflow count based on an unstated assumption about how the OS schedules
	// the worker thread relative to those pushes -- flaky under load (confirmed: it can
	// fail even with no other load at all), and the assumption in the old comment about
	// *why* the count came out the way it did was itself inconsistent with the arithmetic.
	// Make the interleaving deterministic instead: push item 1, then block the test thread
	// until the worker has actually dequeued it and is busy on its 100ms "processing" sleep
	// (signaled from inside the callback, not inferred from timing) before pushing the rest.
	int count = 0;
	std::promise<void> firstItemStarted;
	std::future<void> firstItemStartedFuture = firstItemStarted.get_future();
	std::atomic<bool> signaled = {false};

	Queue<int> queue1([&](int datum){
		if(!signaled.exchange(true))
			firstItemStarted.set_value();
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		count += datum;
	}, 3);

	queue1.push(1);
	firstItemStartedFuture.wait();

	// The queue is now guaranteed empty (item 1 already dequeued) and the worker is busy
	// for 100ms, so these 4 pushes fill a capacity-3 queue starting from empty: only the
	// 4th of them (once the queue is already at capacity) overflows.
	queue1.push(2);
	queue1.push(4);
	queue1.push(8);
	queue1.push(16);

	queue1.wait();

	CHECK(queue1.overflowCounter() == 1);
	CHECK(queue1.queueCounter() == 5);
}

TEST_CASE( "Move-only payloads are supported" ) {
	std::atomic<int> received = {0};
	Queue<std::unique_ptr<int>> queue1([&received](std::unique_ptr<int> datum){
		received += *datum;
	}, 0);

	queue1.push(std::make_unique<int>(1));
	queue1.push(std::make_unique<int>(2));
	queue1.push(std::make_unique<int>(4));

	queue1.wait();

	CHECK(received == 7);
}

TEST_CASE( "Concurrent producers pushing into an unlimited queue" ) {
	constexpr int producerCount = 8;
	constexpr int pushesPerProducer = 200;

	std::atomic<int> sum = {0};
	Queue<int> queue1([&sum](int datum){
		sum += datum;
	}, 0);

	std::vector<std::thread> producers;
	for(int p = 0; p < producerCount; ++p) {
		producers.emplace_back([&queue1]{
			for(int i = 0; i < pushesPerProducer; ++i) queue1.push(1);
		});
	}
	for(auto& producer : producers) producer.join();

	queue1.wait();

	CHECK(sum == producerCount * pushesPerProducer);
	CHECK(queue1.queueCounter() == producerCount * pushesPerProducer);
	CHECK(queue1.overflowCounter() == 0);
}

TEST_CASE( "Queue push() overhead (producer-side, isolated)" ) {
	// Parks the worker on the very first item (blocked on releaseFuture) before timing
	// starts, so the benchmarked push() calls never hand off to a woken consumer. That
	// isolates exactly what push() itself costs - the mutex, the queueCount atomic, and
	// the queue container's insert - from condition-variable wake latency, which would
	// otherwise dominate and hide any change to push()'s internals. This is the number
	// to compare before/after relaxing the atomics' memory order or swapping the
	// backing container.
	std::promise<void> release;
	std::shared_future<void> releaseFuture = release.get_future();
	Queue<int> queue1([releaseFuture](int datum){ releaseFuture.wait(); }, 0);

	queue1.push(0);
	// Let the worker grab item 0 and park on releaseFuture.
	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	BENCHMARK("push (unlimited queue, consumer parked)") {
		queue1.push(1);
	};

	release.set_value();
	queue1.wait();
}

TEST_CASE( "Queue push() overhead, bounded queue (producer-side, isolated)" ) {
	// Same isolation technique as the unlimited-queue benchmark above, but against a
	// bounded queue whose capacity is fully reserved at construction. Comparing the two
	// side by side is the point: a bounded queue's ring buffer never reallocates after
	// construction, so its push() should show tighter, lower-variance timing than the
	// unlimited queue's, which periodically doubles (and copies) its backing storage.
	std::promise<void> release;
	std::shared_future<void> releaseFuture = release.get_future();
	Queue<int> queue1([releaseFuture](int datum){ releaseFuture.wait(); }, 2);

	queue1.push(0);
	// Let the worker grab item 0 and park on releaseFuture.
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	queue1.push(1);

	BENCHMARK("push (bounded queue, consumer parked)") {
		queue1.push(2);
	};

	release.set_value();
	queue1.wait();
}

namespace {
	struct PushLatencyStats {
		double meanNs;
		double p50Ns;
		double p99Ns;
		double maxNs;
	};

	// Times `sampleCount` push() calls against a queue whose worker is parked on the very
	// first item (see the isolated push benchmarks above for why), after `prefill` has run.
	template<typename PrefillFn>
	PushLatencyStats measurePushLatency(unsigned int limit, int sampleCount, PrefillFn prefill) {
		std::promise<void> release;
		std::shared_future<void> releaseFuture = release.get_future();
		Queue<int> queue1([releaseFuture](int){ releaseFuture.wait(); }, limit);

		queue1.push(0);
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		prefill(queue1);

		std::vector<double> samplesNs;
		samplesNs.reserve(sampleCount);
		for(int i = 0; i < sampleCount; ++i) {
			auto start = std::chrono::steady_clock::now();
			queue1.push(i);
			auto end = std::chrono::steady_clock::now();
			samplesNs.push_back(std::chrono::duration<double, std::nano>(end - start).count());
		}

		release.set_value();
		queue1.wait();

		std::vector<double> sorted = samplesNs;
		std::sort(sorted.begin(), sorted.end());
		double sum = 0;
		for(double sample : samplesNs) sum += sample;

		return PushLatencyStats{
			sum / samplesNs.size(),
			sorted[sorted.size() * 50 / 100],
			sorted[sorted.size() * 99 / 100],
			sorted.back()
		};
	}
}

TEST_CASE( "push() latency: bounded queues stay allocation-free, unbounded queues don't" ) {
	// Characterizes (and guards) the central tradeoff of the ring-buffer backing store:
	// a bounded (limit > 0) queue reserves its full capacity at construction and never
	// reallocates again, so its worst-case push() latency should stay flat. An unbounded
	// (limit == 0) queue can't offer that guarantee - its ring buffer must periodically
	// double in size, which means copying every live element - so it will show a much
	// larger tail (max) latency even though its typical-case (p50) cost is similar. This
	// is a known, accepted tradeoff (see Queue.hpp's RingBuffer comment), not a bug; this
	// test exists to keep the contrast visible and to catch a regression in the bounded
	// case specifically, since that's the queue's default and primary use case.
	constexpr int sampleCount = 50000;

	PushLatencyStats bounded = measurePushLatency(2, sampleCount, [](Queue<int>& queue1){
		queue1.push(1);
	});
	PushLatencyStats unbounded = measurePushLatency(0, sampleCount, [](Queue<int>&){});

	WARN("bounded (limit=2):   mean=" << bounded.meanNs << "ns  p50=" << bounded.p50Ns
		<< "ns  p99=" << bounded.p99Ns << "ns  max=" << bounded.maxNs << "ns");
	WARN("unbounded (limit=0): mean=" << unbounded.meanNs << "ns  p50=" << unbounded.p50Ns
		<< "ns  p99=" << unbounded.p99Ns << "ns  max=" << unbounded.maxNs << "ns");

	// A bounded queue never reallocates after construction, so even its worst sample
	// should stay well clear of the unbounded queue's max (typically high hundreds of
	// microseconds to low milliseconds, from its periodic doubling copy). Observed bounded
	// max across repeated local runs was ~250ns-33us; 2ms gives wide headroom for noisy or
	// shared CI hardware (this runs on unknown third-party runners once published) while
	// still being ~2-3 orders of magnitude tighter than the unbounded case, so it stays a
	// meaningful guard against, e.g., someone reintroducing an allocation into the bounded
	// push path.
	CHECK(bounded.maxNs < 2'000'000.0);
}

TEST_CASE( "Queue push()/drain round trip throughput" ) {
	// Unlike the isolated benchmark above, this runs a real, concurrently active
	// consumer, so it also captures worker()'s side of the affected atomics (idle) and
	// the condition-variable hand-off cost, not just push()'s. Each sample pushes a
	// fixed batch and waits for the consumer to fully drain it, so the queue returns to
	// empty/idle before the next sample - the measurement doesn't accumulate state
	// across iterations.
	constexpr int batchSize = 100;
	Queue<int> queue1([](int datum){}, 0);

	BENCHMARK("push + drain a batch of 100 items") {
		for(int i = 0; i < batchSize; ++i) queue1.push(i);
		queue1.wait();
	};
}
