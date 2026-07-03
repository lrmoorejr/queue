#pragma once

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

#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <type_traits>
#include <exception>
#include <iostream>

// Ensure.hpp is an optional dependency: if it's available (either as part of this
// checkout or vendored alongside this header), use its ensure() for a formatted
// diagnostic on failure; otherwise fall back to plain assert() so this header still
// works standalone.
#if __has_include("commons/Ensure.hpp")
	#include "commons/Ensure.hpp"
#elif __has_include("Ensure.hpp")
	#include "Ensure.hpp"
#else
	#include <cassert>
	// Guard against Ensure.hpp having already been included under a path our
	// __has_include checks above don't know about (e.g. vendored elsewhere as
	// "3rdparty/Ensure.hpp") -- COMMONS_ENSURE_HPP is defined by Ensure.hpp
	// itself, so this still catches that case even under an unknown filename.
	#if !defined(COMMONS_ENSURE_HPP) && !defined(ensure)
		#define ensure(condition, ...) assert((condition))
	#endif
#endif

namespace thr {
	/**
	 * @brief Queue is a bounded, thread-safe producer/consumer work queue backed by a single
	 * background worker thread. Producers call push() from any thread; the worker thread
	 * calls the work function supplied at construction for each item, in FIFO order. When
	 * bounded (limit > 0, the default) and full, push() overwrites the most recently queued
	 * item rather than blocking or growing, so the newest data is never lost to overflow -
	 * see push(). Use limit == 0 for an unbounded queue that never overflows.
	 */
	template<typename T>
	class Queue {
		static_assert(std::is_move_constructible_v<T> && std::is_move_assignable_v<T>,
			"thr::Queue<T> requires T to be move-constructible and move-assignable");

	public:
		/**
		 * @brief Constructs the queue and starts its background worker thread.
		 *
		 * @param workFunction Callback invoked on the worker thread for each pushed item, in
		 * FIFO order. May be nullptr (the default), in which case pushed items are silently
		 * discarded once dequeued. An exception that escapes workFunction terminates the
		 * process (this is standard behavior for any exception that escapes a thread's entry
		 * function) - catch anything workFunction can throw inside workFunction itself.
		 * @param limit Maximum number of items the queue will hold before push() starts
		 * overwriting the most recently queued item (see push()). 0 means unbounded. Defaults
		 * to 1.
		 */
		Queue(const std::function<void(T)> workFunction = nullptr, unsigned int limit = 1) : workFunction(workFunction), limit(limit) {
			// Reserve the full bounded capacity up front so push()/pop() never allocate
			// once construction is done. Only meaningful for limit > 0 - a limit == 0
			// (unlimited) queue has no fixed ceiling to reserve and grows lazily instead.
			if(limit > 0) queue.reserveFixed(limit);
			workerThread = std::thread(&Queue::worker, this);
		}

		/**
		 * @brief Signals the worker thread to stop and joins it. Any item still queued at the
		 * time of destruction is discarded without being passed to the work function; call
		 * wait() first if the queue must be fully drained before destruction.
		 */
		~Queue() {
			std::unique_lock<std::mutex> lock(mutex);
			terminate.store(true, std::memory_order_relaxed);
			lock.unlock();

			conditional.notify_all();
			workerThread.join();
		}

		/**
		 * @brief Adds an item to the queue for the worker thread to process. If the queue is
		 * bounded (limit > 0) and already at capacity, the most recently queued item is
		 * overwritten in place with `input` instead of growing the queue or blocking the
		 * caller - the newest data pushed is never lost to overflow, though older,
		 * not-yet-processed items may be. Safe to call concurrently from multiple threads.
		 *
		 * @param input The item to enqueue (or to overwrite the queue's newest item with, on
		 * overflow).
		 */
		void push(T input) {
			std::unique_lock<std::mutex> lock(mutex);
			queueCount.fetch_add(1, std::memory_order_relaxed);
			if(limit > 0 && queue.size() >= limit) {
				ensure(queue.size() == limit);
				overflowCount.fetch_add(1, std::memory_order_relaxed);

				// Replace the last item in the queue with this new one.
				// In this way, the most recent data received will never be lost due to overflow.
				queue.replaceBack(std::move(input));
			} else {
				queue.pushBack(std::move(input));
			}
			lock.unlock();

			// Let the thread know that work is available
			conditional.notify_one();
		}

		/**
		 * @brief Blocks the calling thread until the queue is empty and the worker thread is
		 * idle (i.e. all previously pushed items have been fully processed). Returns
		 * immediately if that is already true. Also returns once the queue is being destroyed.
		 */
		void wait() {
			std::unique_lock<std::mutex> lock(mutex);
			emptyConditional.wait(lock, [this]{
				return (queue.empty() || terminate.load(std::memory_order_relaxed))
					&& idle.load(std::memory_order_relaxed);
			});
			lock.unlock();
		}

		/**
		 * @brief Queries whether the queue is at capacity, i.e. the next push() would
		 * overwrite the newest item rather than growing the queue. Always false for an
		 * unbounded queue (limit == 0). The result may be stale immediately after it's
		 * returned, since other threads can push or the worker can drain concurrently.
		 *
		 * @return true The queue is bounded and at capacity.
		 * @return false The queue has room for more items (or is unbounded).
		 */
		bool isFull() {
			std::lock_guard<std::mutex> lock(mutex);
			return limit > 0 && queue.size() >= limit;
		}

		/**
		 * @brief Returns the number of push() calls since the last call to queueCounter(),
		 * then resets the count to 0. Intended for periodic rate monitoring.
		 *
		 * @return Number of pushes since the last call to queueCounter() (or since
		 * construction, on the first call).
		 */
		int queueCounter() {
			return queueCount.exchange(0, std::memory_order_relaxed);
		}

		/**
		 * @brief Returns the number of push() calls that overwrote the newest queued item due
		 * to the queue being full, since the last call to overflowCounter(), then resets the
		 * count to 0. Intended for periodic rate monitoring.
		 *
		 * @return Number of overflowing pushes since the last call to overflowCounter() (or
		 * since construction, on the first call).
		 */
		int overflowCounter() {
			return overflowCount.exchange(0, std::memory_order_relaxed);
		}

		/**
		 * @brief Queries whether the worker thread is currently idle, i.e. not in the middle
		 * of running the work function for an item. Unlike wait()'s notion of "drained", this
		 * does not also require the queue to be empty - use wait() to block until both are
		 * true. Safe to call without locking; the result may be stale immediately after it's
		 * returned.
		 *
		 * @return true The worker thread is not currently processing an item.
		 * @return false The worker thread is currently running the work function.
		 */
		bool isIdle() {
			return idle.load(std::memory_order_relaxed);
		}

		/**
		 * @brief Queries whether there is at least one item currently queued and waiting to be
		 * processed. Does not reflect an item the worker thread is actively processing (which
		 * has already been dequeued) - see isIdle().
		 *
		 * @return true There is at least one item queued.
		 * @return false The queue is empty.
		 */
		bool hasWork() {
			std::lock_guard<std::mutex> lock(mutex);
			return !queue.empty();
		}

		/**
		 * @brief Returns the number of items currently queued and waiting to be processed.
		 * Does not count an item the worker thread is actively processing (which has already
		 * been dequeued).
		 *
		 * @return Current queue length.
		 */
		size_t queuedWork() {
			std::lock_guard<std::mutex> lock(mutex);
			return queue.size();
		}

	private:
		void worker() {
			// Loop on `true`, not `!terminate`: termination must always be handled by
			// re-acquiring the lock below and going through the `if(terminate)` branch,
			// which releases any thread blocked in wait(). Looping on `!terminate` here
			// would let the thread exit mid-callback (queue empty, idle already false)
			// without ever waking a concurrent wait() caller.
			while(true) {
				std::unique_lock<std::mutex> lock(mutex);

				// If the queue is empty, wait for work or termination
				if(queue.empty() && !terminate.load(std::memory_order_relaxed)) {
					idle.store(true, std::memory_order_relaxed);
					emptyConditional.notify_one();
					conditional.wait(lock, [this]{
						return !queue.empty() || terminate.load(std::memory_order_relaxed);
					});
				}

				// We either have work or termination request.  Handle termination.
				if(terminate.load(std::memory_order_relaxed)) {
					idle.store(true, std::memory_order_relaxed);
					lock.unlock();
					emptyConditional.notify_all();
					break;
				}

				// At this point we must have work in the queue.  Get it.
				ensure(!queue.empty());
				idle.store(false, std::memory_order_relaxed);
				T workItem = queue.popFront();
				lock.unlock();

				callWorkFunction(std::move(workItem));
			}
		}

		// An exception escaping a std::thread's entry function is required by the standard to
		// call std::terminate() - so an uncaught exception from workFunction was already fatal
		// before this try/catch existed. Catching it here doesn't change that outcome; it only
		// replaces the generic "terminate called after throwing an instance of ..." with a
		// diagnostic that actually says which callback caused it. If workFunction can throw in
		// a way that's expected rather than a bug, catch it inside workFunction itself instead.
		void callWorkFunction(T workItem) {
			if(workFunction == nullptr) return;
			try {
				workFunction(std::move(workItem));
			} catch(const std::exception& e) {
				std::cerr << "thr::Queue: workFunction threw an uncaught exception (" << e.what()
					<< "); terminating - see Queue.hpp's callWorkFunction() comment.\n";
				std::terminate();
			} catch(...) {
				std::cerr << "thr::Queue: workFunction threw a non-standard uncaught exception; "
					<< "terminating - see Queue.hpp's callWorkFunction() comment.\n";
				std::terminate();
			}
		}

		// Fixed-capacity (limit > 0) or geometrically-growing (limit == 0) circular buffer.
		// Once a physical slot has been constructed it's reused via move-assignment instead
		// of being reallocated, so a bounded queue - the common case, since limit defaults to
		// 1 - never allocates again after reserveFixed() runs in the constructor. Not
		// thread-safe on its own; every access is made under Queue::mutex.
		class RingBuffer {
		public:
			void reserveFixed(size_t fixedCapacity) {
				storage.reserve(fixedCapacity);
				capacity = fixedCapacity;
			}

			bool empty() const { return itemCount == 0; }
			size_t size() const { return itemCount; }

			// Appends a new logical element, growing the backing storage first if it's
			// already full. Growth is only ever needed for an unlimited (limit == 0) queue -
			// a fixed-capacity queue's push() only calls this while size() < capacity.
			void pushBack(T value) {
				if(itemCount == capacity) grow();
				size_t slot = (head + itemCount) % capacity;
				if(slot == storage.size()) storage.push_back(std::move(value));
				else storage[slot] = std::move(value);
				++itemCount;
			}

			// Overwrites the most-recently-pushed element in place, without growing
			// itemCount. Used for the "keep the newest" overflow path on a full,
			// fixed-capacity buffer, so it never needs to grow.
			void replaceBack(T value) {
				storage[(head + itemCount - 1) % capacity] = std::move(value);
			}

			T popFront() {
				T value = std::move(storage[head]);
				head = (head + 1) % capacity;
				--itemCount;
				return value;
			}

		private:
			void grow() {
				size_t newCapacity = capacity == 0 ? 4 : capacity * 2;
				std::vector<T> newStorage;
				newStorage.reserve(newCapacity);
				for(size_t i = 0; i < itemCount; ++i) {
					newStorage.push_back(std::move(storage[(head + i) % capacity]));
				}
				storage = std::move(newStorage);
				capacity = newCapacity;
				head = 0;
			}

			std::vector<T> storage;
			size_t capacity = 0;
			size_t head = 0;
			size_t itemCount = 0;
		};

		std::mutex mutex;

		std::thread workerThread;

		// terminate/idle are only ever touched while holding `mutex` (isIdle() is the
		// sole exception - an advisory, unlocked read). The mutex already provides all
		// the happens-before ordering these need, so every access below uses
		// memory_order_relaxed: atomicity to make the unlocked read in isIdle() well
		// defined, without paying for a barrier the mutex has already established.
		std::atomic<bool> terminate = {false};
		std::atomic<bool> idle = {true};
		const std::function<void(T)> workFunction;
		const unsigned int limit;	// 0 indicates no limit

		RingBuffer queue;
		std::condition_variable conditional;
		std::condition_variable emptyConditional;

		// Rate monitoring counters; reset to 0 each time they're read via
		// queueCounter()/overflowCounter().
		std::atomic<int> overflowCount = {0};
		std::atomic<int> queueCount = {0};
	};

}
