# Queue.hpp

[API docs](https://lrmoorejr.github.io/queue/)

A bounded, thread-safe producer/consumer work queue backed by a single background worker
thread. Push items onto it from any thread; the worker thread calls your callback for each one,
in FIFO order.

**By default it only holds one item.** `Queue`'s default capacity is 1, not unbounded like
`std::queue` -- if a second item is pushed before the worker has processed the first, `push()`
overwrites that single slot in place rather than blocking or growing, so the newest data pushed
is never lost to overflow, but older, not-yet-processed data can be. Pass a larger (or `0`, for
unbounded) `limit` if that's not what you want -- see [Bounded vs. unbounded](#bounded-vs-unbounded)
below.

```cpp
#include <cstdio>
#include "Queue.hpp"

thr::Queue<int> queue([](int value) {
    printf("got %d\n", value);
});

queue.push(42);
queue.wait(); // block until the worker has processed everything pushed so far
```

## Requirements

- C++20 or later
- Header-only -- copy `Queue.hpp` into your project and `#include` it
- Links against threading support -- on Linux, compile/link with `-pthread` (or your build
  system's equivalent); without it, `std::thread`/`std::mutex` usage can build but throw
  `std::system_error` at runtime instead of failing to link
- Optional: [`Ensure.hpp`](https://github.com/lrmoorejr/ensure) for a formatted diagnostic on a
  usage error; if it's not present, Queue falls back to plain `assert()` (see below)

## API

| Call | Behavior |
|---|---|
| `push(T)` | Enqueues an item for the worker thread. If the queue is bounded and already full, overwrites the newest queued item in place instead of growing or blocking. |
| `wait()` | Blocks the caller until the queue is empty and the worker is idle. |
| `isFull()` | Whether the next `push()` would overwrite rather than grow. Always false when unbounded. |
| `isIdle()` | Whether the worker is between callbacks right now. Lock-free read. |
| `hasWork()` | Whether at least one item is currently queued (not counting one the worker is actively processing). |
| `queuedWork()` | Current queue length. |
| `queueCounter()` | Number of pushes since the last call; resets to 0. |
| `overflowCounter()` | Number of overwriting (overflow) pushes since the last call; resets to 0. |

### Bounded vs. unbounded

```cpp
thr::Queue<int> bounded(callback);        // limit defaults to 1
thr::Queue<int> bounded(callback, 8);     // holds up to 8 items
thr::Queue<int> unbounded(callback, 0);   // grows to fit; never overflows
```

A bounded queue reserves its full capacity at construction and never allocates again --
`push()` and the worker's dequeue are allocation-free for the entire lifetime of the queue. An
unbounded queue (`limit == 0`) grows geometrically as needed, same amortized cost as
`std::vector`, but pays an occasional larger copy when it doubles in size.

### Thread safety

Every public method is safe to call from any thread. Exactly one worker thread runs your
callback, spawned in the constructor and joined in the destructor. Any item still queued when
the queue is destroyed is discarded without running the callback -- call `wait()` first if the
queue needs to be fully drained before destruction.

**An exception that escapes your callback terminates the process.** This is standard C++
behavior for any exception that escapes a thread's entry function, not something Queue adds --
but Queue does catch it first to print which callback threw and why, before letting it proceed
to terminate, so you get more than a generic "terminate called after throwing an instance of
...". If your callback can throw in a way that's expected rather than a bug, catch it inside the
callback itself.

### Requirements on `T`

`T` must be move-constructible and move-assignable (checked with a `static_assert`, so passing
a type that isn't gives an immediate, readable compile error rather than a template-instantiation
wall of text). Copyability and default-constructibility are not required -- move-only types like
`std::unique_ptr<U>` work.

## Pairs well with Router.hpp

[`Router.hpp`](https://github.com/lrmoorejr/router) and `Queue.hpp` share the same "any type can
be the payload" design -- Router keys handlers by `std::type_index`, Queue is a plain template
on the item type -- so they compose naturally: dispatch a `Router` event onto a background
`Queue` to do the (possibly slow) work off the calling thread, without changing how either type
is declared or used elsewhere.

```cpp
struct JobRequested { int jobId; };

thr::Queue<JobRequested> jobQueue([](JobRequested job) {
    // do the (possibly slow) work off the calling thread
});

rt::Router router;
router.addHandler<JobRequested>([&jobQueue](const JobRequested& event) {
    jobQueue.push(event);
});

router.handle(JobRequested{.jobId = 7});
```

## Ensure.hpp fallback

Queue uses a couple of runtime checks -- internal invariants, not usage errors reachable from
the public API. If [`Ensure.hpp`](https://github.com/lrmoorejr/ensure) is available -- either
checked out alongside `Queue.hpp`, or reachable as `commons/Ensure.hpp` -- Queue uses its
`ensure()` for a formatted diagnostic. Otherwise it falls back to plain `assert()`. Either way,
these checks compile out entirely in release (`NDEBUG`) builds, matching `assert()`'s usual
behavior.

## License

Apache License 2.0 -- see [LICENSE](LICENSE).
