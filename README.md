# LockFree.AsyncChannel

A lock-free, multi-producer single-consumer `AsyncChannel` for Swift Concurrency, designed as a replacement for `AsyncStream`.

- **Low contention.** Producers claim slots with a single CAS on a shared tail word and don't take a lock, except for a brief spin while the producer that claims the last slot of a block installs the next block. The consumer never waits on a lock.
- **Multi-producer, single-consumer.** You can copy the `Sink` and share it between any number of tasks. The channel itself is `~Copyable`, so it always has exactly one consumer.
- **Synchronous `send`.** A producer doesn't need an `async` context, so you can call `send` from callbacks, delegates or C code.
- **Optional backpressure** with low and high watermarks.
- **Non-blocking `tryNext()` and time-limited `next(timeout:)` receivers.**

## Why not `AsyncStream` or `MultiProducerSingleConsumerAsyncChannel`?

Swift already has two channels of this kind: Foundation/stdlib [`AsyncStream`](https://developer.apple.com/documentation/swift/asyncstream) and [`MultiProducerSingleConsumerAsyncChannel`](https://github.com/apple/swift-async-algorithms) from swift-async-algorithms. `AsyncChannel` differs from both in API and in speed.

### More ways to receive

|                                       | `AsyncChannel` | `AsyncStream` | `MultiProducerSingleConsumerAsyncChannel` |
|---------------------------------------|:--------------:|:-------------:|:-----------------------------------------:|
| `await next()`                        | ✅             | ✅            | ✅                                        |
| `tryNext()`: non-blocking receive     | ✅             | ❌            | ❌                                        |
| `next(timeout:)`: receive with timeout | ✅             | ❌            | ❌                                        |
| Synchronous `send` from any thread    | ✅             | ✅            | ✅                                        |
| Backpressure                          | ✅ watermarks  | ❌ (only dropping buffer policies) | ✅ watermarks       |

With the other two channels, polling or a timeout means racing `next()` against a sleeping task in a task group. That adds a task, an allocation and a cancellation on every call, and cancelling `next()` can terminate the whole stream. `AsyncChannel` provides both operations directly.

### Much faster

The benchmark in [`Benchmarks/`](Benchmarks) uses [package-benchmark](https://github.com/ordo-one/benchmark). In every iteration one producer sends 1,000,000 `Int`s while one consumer task receives them. Creating the channel and starting the consumer are not measured. It measures two things:

- **dispatch:** the time the producer needs to send all elements.
- **first dispatch to last receive:** the time from the first `send` until the consumer has the last element.

Apple M3 Max, macOS 27, Swift 6.4, release build, 20 iterations after 2 warm-up iterations (9 for `AsyncStream`, which reaches the 10 s limit first). All values are per element:

**Dispatch**

| Channel                                   | wall p0 ns | wall p50 ns | wall p100 ns | CPU p50 ns | mallocs |
|-------------------------------------------|-----------:|------------:|-------------:|-----------:|--------:|
| **AsyncChannel**                          | **6**      | **6**       | **6**        | **6**      | **0**   |
| AsyncStream                               | 742        | 893         | 1018         | 1418       | 1       |
| MultiProducerSingleConsumerAsyncChannel   | 235        | 277         | 396          | 516        | 1       |

**First dispatch to last receive**

| Channel                                   | wall p0 ns | wall p50 ns | wall p100 ns | CPU p50 ns | mallocs |
|-------------------------------------------|-----------:|------------:|-------------:|-----------:|--------:|
| **AsyncChannel**                          | **26**     | **26**      | **26**       | **32**     | **0**   |
| AsyncStream                               | 1138       | 1145        | 1193         | 1648       | 2       |
| MultiProducerSingleConsumerAsyncChannel   | 285        | 333         | 385          | 607        | 1       |

At the median, `AsyncChannel` sends about **46× faster** than `MultiProducerSingleConsumerAsyncChannel` and **149× faster** than `AsyncStream`. End to end it is about **13×** and **44×** faster. It also does no allocation per element: elements go into blocks that each hold many slots, so the allocation count rounds to 0 per element.

The difference comes from the design. `AsyncStream` and `MultiProducerSingleConsumerAsyncChannel` take a lock on every `send` and `next`, and their consumer and producers contend for it. In `AsyncChannel`, a producer reserves a slot in a linked list of fixed-size blocks with a CAS on the tail word, writes the element in place and marks the slot as written. The consumer reads slots in order, without locks. It suspends only when the next slot is still empty, and the producer that fills that slot resumes it.

To run the benchmark yourself (package-benchmark needs `jemalloc`, e.g. `brew install jemalloc`):

```sh
cd Benchmarks
swift package benchmark                                  # all cases, 1,000,000 elements per iteration
swift package benchmark --filter "AsyncChannel.*"        # only some cases
swift package benchmark baseline update main             # save a baseline...
swift package benchmark baseline compare main            # ...and compare against it later
```

## Requirements

- Swift 6.0+
- macOS 13+ / iOS 16+

## Installation

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/ser-0xff/package-lock-free.git", branch: "main"),
],
targets: [
    .target(
        name: "MyAmazingTarget",
        dependencies: [
            .product(name: "LockFree", package: "package-lock-free"),
        ]
    ),
]
```

## Usage

### Basic

```swift
import LockFree

let channel = AsyncChannel<Int>()
let sink = channel.makeSink()

Task {
    for i in 0..<10 {
        sink.send(i)
    }
    sink.finish()
}

while let value = await channel.next() {
    print(value)
}
```

`next()` returns `nil` after the channel is finished and every element sent before `finish()` has been delivered.

### Multiple producers

`Sink` is a `Sendable` struct, so you can hand it to as many tasks as you need:

```swift
let channel = AsyncChannel<String>()
let sink = channel.makeSink()

await withTaskGroup(of: Void.self) { group in
    for worker in 0..<4 {
        group.addTask {
            sink.send("hello from \(worker)")
        }
    }
}
sink.finish()

while let message = await channel.next() {
    print(message)
}
```

### Non-blocking receive: `tryNext()`

`tryNext()` returns immediately and never suspends:

```swift
switch channel.tryNext() {
case .element(let value):
    handle(value)
case .empty:
    // The channel is open but has nothing to deliver right now.
    break
case .finished:
    // The channel is finished and drained.
    break
}
```

Use it to drain a batch without suspending, or to poll from code that isn't `async`.

### Receive with a timeout: `next(timeout:)`

```swift
switch await channel.next(timeout: .milliseconds(100)) {
case .element(let value):
    handle(value)
case .timedOut:
    // No element arrived within 100 ms. The channel is still open.
    break
case .finished:
    // The channel is finished and drained.
    break
}
```

### Backpressure

Create the channel with watermarks to stop producers from outrunning the consumer:

```swift
let channel = AsyncChannel<Data>(lowWatermark: 16, highWatermark: 64)
let sink = channel.makeSink()

Task {
    for chunk in chunks {
        switch sink.send(chunk) {
        case .sent:
            continue
        case .backpressure:
            // The element is sent, but the buffer reached the high watermark.
            // Wait until the consumer takes it down to the low watermark.
            guard await sink.waitForDemand() else { return }
        case .closed:
            return
        }
    }
    sink.finish()
}
```

`send` is still synchronous and always accepts the element unless the channel is closed. It only *reports* backpressure, so the producer decides what to do. `waitForDemand()` returns `false` if the channel is closed or the waiting task is cancelled.

### Lifetime and cancellation

- `sink.finish()` closes the channel. Elements already sent are still delivered, and after them `next()` returns `nil`. Later `send` calls return `.closed`.
- Dropping the `AsyncChannel` (the consumer) also closes it. Producers waiting in `waitForDemand()` are resumed with `false`.
- Cancelling the task that is suspended in `next()` closes the channel.

## How it works

The core is written in C11 atomics (`Sources/CLockFree`). The Swift wrapper (`Sources/LockFree`) connects it to continuations.

- Elements are stored in slots of a linked list of blocks of 32 slots. A single 64-bit `tail_state` word holds the next slot number plus a *closed* bit and a *locked* bit. The locked bit is set while one producer installs the next block.
- Each slot has a small state: empty, consumer parked, written or closed. The consumer parks on the first slot that isn't written yet, and the producer that writes that slot resumes it.
- For backpressure, the number of buffered elements is the number of claimed slots minus the number of consumed ones. The backpressured mode and the stack of waiting producers share one atomic word, so a producer can't be queued after the waiters were resumed.

## License

See [LICENSE](LICENSE).
