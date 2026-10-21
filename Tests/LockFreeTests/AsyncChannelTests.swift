import Testing
import LockFree
import CLockFree
import os

// cancel a parked wait, then drop the channel
@Test func cancelParkedThenDrop() async {
    let task = Task { () -> Int? in
        let asyncChannel = AsyncChannel<Int>()
        _ = asyncChannel.makeSink()
        return await asyncChannel.next()
    }
    try? await Task.sleep(for: .milliseconds(100))
    task.cancel()
    #expect(await task.value == nil)
}


// cancelling a parked wait closes the channel
@Test func cancelParkedThenSend() async {
    let task = Task { () -> (Int?, AsyncChannel<Int>.SendResult, Int?, Int?) in
        let asyncChannel = AsyncChannel<Int>()
        let sink = asyncChannel.makeSink()
        let a = await asyncChannel.next()
        let sent = sink.send(7)
        let b = await asyncChannel.next()
        let c = await asyncChannel.next()
        return (a, sent, b, c)
    }
    try? await Task.sleep(for: .milliseconds(100))
    task.cancel()
    let result = await task.value
    #expect(result.0 == nil)
    #expect(result.1 == .closed)
    #expect(result.2 == nil)
    #expect(result.3 == nil)
}


// task is already cancelled before the first wait: buffered elements are
// still delivered, then the channel is closed
@Test func alreadyCancelled() async {
    let task = Task { () -> (Int?, Int?, Int?, AsyncChannel<Int>.SendResult, Int?) in
        let asyncChannel = AsyncChannel<Int>()
        let sink = asyncChannel.makeSink()
        while !Task.isCancelled { await Task.yield() }
        sink.send(7)
        sink.send(8)
        let a = await asyncChannel.next()
        let b = await asyncChannel.next()
        let c = await asyncChannel.next()
        let sent = sink.send(9)
        let d = await asyncChannel.next()
        return (a, b, c, sent, d)
    }
    task.cancel()
    let result = await task.value
    #expect(result.0 == 7)
    #expect(result.1 == 8)
    #expect(result.2 == nil)
    #expect(result.3 == .closed)
    #expect(result.4 == nil)
}


// cancel racing with a sender, many rounds
@Test func cancelRacesSend() async {
    for i in 0..<2000 {
        let asyncChannel = AsyncChannel<Int>()
        let sink = asyncChannel.makeSink()
        let task = Task { () -> Int? in
            await asyncChannel.next()
        }
        let sender = Task.detached { _ = sink.send(i) }
        if i % 2 == 0 { await Task.yield() }
        task.cancel()
        let next = await task.value
        await sender.value
        #expect(next == nil || next == i)
    }
}


struct Padded16: Sendable, Equatable {
    var tag: Int8
    var value: SIMD2<Double>
}

private func roundTrip<Element: Sendable & Equatable>(_ values: [Element]) async {
    let asyncChannel = AsyncChannel<Element>()
    let sink = asyncChannel.makeSink()
    for value in values {
        sink.send(value)
    }
    for value in values {
        let next = await asyncChannel.next()
        #expect(next == value)
    }
}

// elements aligned by 16 are stored at properly aligned addresses
@Test func alignedBy16() async {
    #expect(MemoryLayout<SIMD4<Float>>.alignment == 16)
    #expect(MemoryLayout<Padded16>.alignment == 16)
    await roundTrip((0..<100).map { SIMD4<Float>(repeating: Float($0)) })
    await roundTrip((0..<100).map { Padded16(tag: Int8($0), value: SIMD2(repeating: Double($0))) })
}


// Swift types never exceed alignment 16, so drive the over-aligned path through the C API
@Test func overAligned() {
    for alignment in [32, 64, 4096] {
        var consumerData: UnsafeMutableRawPointer?
        let asyncChannel = async_channel_create(24, alignment, 0, &consumerData, { _ in }, 0, 0, { _, _ in })
        var ptrs: [UnsafeMutableRawPointer] = []
        // enough to span several blocks
        for _ in 0..<200 {
            var position: UInt64 = 0
            let ptr = async_channel_reserve(asyncChannel, &position)!
            #expect(UInt(bitPattern: ptr) % UInt(alignment) == 0)
            ptr.initializeMemory(as: UInt8.self, repeating: 0xAB, count: 24)
            #expect(async_channel_send(asyncChannel, ptr, position) == 0)
            ptrs.append(ptr)
        }
        for ptr in ptrs {
            #expect(async_channel_next(asyncChannel) == ptr)
        }
        #expect(async_channel_next(asyncChannel) == nil)
        async_channel_finish(asyncChannel)
        var position: UInt64 = 0
        #expect(async_channel_reserve(asyncChannel, &position) == nil)
        #expect(async_channel_is_closed(asyncChannel, async_channel_next(asyncChannel)) != 0)
        async_channel_destroy(asyncChannel)
    }
}


// wake up a parked consumer with an aligned element
@Test func alignedWakeUp() async throws {
    let asyncChannel = AsyncChannel<SIMD4<Float>>()
    let sink = asyncChannel.makeSink()
    _ = Task {
        try await Task.sleep(for: .milliseconds(100))
        sink.send(SIMD4(repeating: 42))
    }
    let next = await asyncChannel.next()
    #expect(next == SIMD4(repeating: 42))
}


// drop the channel with unconsumed aligned elements
@Test func alignedDropUnconsumed() async {
    let asyncChannel = AsyncChannel<SIMD4<Float>>()
    let sink = asyncChannel.makeSink()
    for i in 0..<10 {
        sink.send(SIMD4(repeating: Float(i)))
    }
    let next = await asyncChannel.next()
    #expect(next == SIMD4(repeating: 0))
}


// elements sent before finish are delivered, then next returns nil
@Test func finishAfterSend() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    #expect(sink.send(1) == .sent)
    #expect(sink.send(2) == .sent)
    sink.finish()
    #expect(sink.send(3) == .closed)
    #expect(await asyncChannel.next() == 1)
    #expect(await asyncChannel.next() == 2)
    #expect(await asyncChannel.next() == nil)
    #expect(await asyncChannel.next() == nil)
}


// finish on an empty channel, repeated and from another sink
@Test func finishEmpty() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    sink.finish()
    sink.finish()
    asyncChannel.makeSink().finish()
    #expect(await asyncChannel.next() == nil)
    #expect(sink.send(1) == .closed)
    #expect(await asyncChannel.next() == nil)
}


// finish after the consumer has drained the channel
@Test func finishAfterDrain() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    sink.send(1)
    #expect(await asyncChannel.next() == 1)
    sink.finish()
    #expect(await asyncChannel.next() == nil)
    #expect(await asyncChannel.next() == nil)
}


// finish wakes up a parked consumer
@Test func finishWakesParked() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    _ = Task {
        try await Task.sleep(for: .milliseconds(100))
        sink.finish()
    }
    #expect(await asyncChannel.next() == nil)
    #expect(await asyncChannel.next() == nil)
}


// dropping a finished channel which was never consumed
@Test func finishThenDrop() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    sink.send(1)
    sink.finish()
}


final class Tracked: Sendable {
    typealias Counter = OSAllocatedUnfairLock<Int>

    let live: Counter

    init(_ live: Counter) {
        self.live = live
        live.withLock { $0 += 1 }
    }

    deinit {
        live.withLock { $0 -= 1 }
    }
}

// the channel is released together with the last task holding its sink,
// which may happen slightly after the task's value is available
private func expectAllReleased(_ live: Tracked.Counter) async {
    for _ in 0..<1000 where live.withLock({ $0 }) != 0 {
        try? await Task.sleep(for: .milliseconds(1))
    }
    #expect(live.withLock { $0 } == 0)
}

// finish racing with senders: exactly the accepted elements are delivered,
// and everything is released
@Test func finishRacesSend() async {
    for round in 0..<500 {
        let live = Tracked.Counter(initialState: 0)
        do {
            let asyncChannel = AsyncChannel<Tracked>()
            let sink = asyncChannel.makeSink()
            let senders = (0..<4).map { _ in
                Task.detached { () -> Int in
                    var accepted = 0
                    for _ in 0..<50 {
                        if sink.send(Tracked(live)) != .closed { accepted += 1 }
                    }
                    return accepted
                }
            }
            let finisher = Task.detached {
                if round % 2 == 0 { await Task.yield() }
                sink.finish()
            }
            var received = 0
            // consume only some rounds completely, to leave elements for deinit
            while round % 3 != 0, await asyncChannel.next() != nil {
                received += 1
            }
            var accepted = 0
            for sender in senders { accepted += await sender.value }
            await finisher.value
            if round % 3 != 0 {
                #expect(received == accepted)
            }
        }
        await expectAllReleased(live)
    }
}


// cancellation racing with senders releases everything as well
@Test func cancelRacesSendReleases() async {
    for round in 0..<500 {
        let live = Tracked.Counter(initialState: 0)
        do {
            let asyncChannel = AsyncChannel<Tracked>()
            let sink = asyncChannel.makeSink()
            let sender = Task.detached {
                for _ in 0..<20 { sink.send(Tracked(live)) }
            }
            let task = Task { () -> Int in
                var received = 0
                while await asyncChannel.next() != nil { received += 1 }
                return received
            }
            if round % 2 == 0 { await Task.yield() }
            task.cancel()
            #expect(await task.value <= 20)
            await sender.value
        }
        await expectAllReleased(live)
    }
}


// more elements than a block holds, sent ahead and then consumed
@Test func manyBlocks() async {
    await roundTrip(Array(0..<10_000))
}


// the consumer parks before every element, including at the block boundaries
@Test func parkedOnEveryElement() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    let sender = Task.detached {
        for i in 0..<300 {
            try? await Task.sleep(for: .microseconds(200))
            sink.send(i)
        }
        sink.finish()
    }
    var expected = 0
    while let next = await asyncChannel.next() {
        #expect(next == expected)
        expected += 1
    }
    #expect(expected == 300)
    await sender.value
}


// closing exactly at the block boundaries and around them
@Test func finishAtBlockBoundary() async {
    for count in [30, 31, 32, 33, 61, 62, 63, 64, 65, 126, 127] {
        let asyncChannel = AsyncChannel<Int>()
        let sink = asyncChannel.makeSink()
        for i in 0..<count { sink.send(i) }
        sink.finish()
        #expect(sink.send(-1) == .closed)
        for i in 0..<count {
            #expect(await asyncChannel.next() == i)
        }
        #expect(await asyncChannel.next() == nil)
        #expect(await asyncChannel.next() == nil)
    }
}


// several producers: nothing is lost and every producer's elements keep their order
@Test func multipleProducers() async {
    let producers = 8
    let perProducer = 20_000
    let asyncChannel = AsyncChannel<(producer: Int, value: Int)>()
    let sink = asyncChannel.makeSink()
    let tasks = (0..<producers).map { producer in
        Task.detached {
            for value in 0..<perProducer {
                sink.send((producer, value))
            }
        }
    }
    let finisher = Task.detached {
        for task in tasks { await task.value }
        sink.finish()
    }
    var expected = [Int](repeating: 0, count: producers)
    while let next = await asyncChannel.next() {
        #expect(next.value == expected[next.producer])
        expected[next.producer] += 1
    }
    #expect(expected == [Int](repeating: perProducer, count: producers))
    await finisher.value
}


@Test func nextWithTimeoutTimesOut() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    #expect(await asyncChannel.next(timeout: .milliseconds(100)) == .timedOut)
    // the channel is still usable after a timeout
    sink.send(5)
    #expect(await asyncChannel.next(timeout: .seconds(1)) == .element(5))
    sink.finish()
    #expect(await asyncChannel.next(timeout: .seconds(1)) == .finished)
}


@Test func nextWithTimeoutReceives() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    _ = Task {
        try await Task.sleep(for: .milliseconds(100))
        sink.send(42)
    }
    #expect(await asyncChannel.next(timeout: .seconds(5)) == .element(42))
}


// timeouts racing with sends: every element is received exactly once
@Test func nextWithTimeoutRace() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    let count = 20_000
    _ = Task.detached {
        for idx in 0..<count {
            sink.send(idx)
            if idx % 64 == 0 {
                try? await Task.sleep(for: .microseconds(50))
            }
        }
        sink.finish()
    }
    var expected = 0
    loop: while true {
        switch await asyncChannel.next(timeout: .microseconds(20)) {
        case .element(let value):
            #expect(value == expected)
            expected += 1
        case .timedOut:
            continue
        case .finished:
            break loop
        }
    }
    #expect(expected == count)
}


@Test func tryNext() {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    #expect(asyncChannel.tryNext() == .empty)
    sink.send(1)
    #expect(asyncChannel.tryNext() == .element(1))
    #expect(asyncChannel.tryNext() == .empty)
    sink.send(2)
    sink.send(3)
    sink.finish()
    #expect(asyncChannel.tryNext() == .element(2))
    #expect(asyncChannel.tryNext() == .element(3))
    #expect(asyncChannel.tryNext() == .finished)
    #expect(asyncChannel.tryNext() == .finished)
}


// tryNext() takes the count down to the low watermark and resumes the waiter
@Test func tryNextResumesWaiter() async {
    let asyncChannel = AsyncChannel<Int>(lowWatermark: 1, highWatermark: 3)
    let sink = asyncChannel.makeSink()
    sink.send(1)
    sink.send(2)
    #expect(sink.send(3) == .backpressure)
    let producer = Task { await sink.waitForDemand() }
    try? await Task.sleep(for: .milliseconds(50))
    #expect(asyncChannel.tryNext() == .element(1))
    #expect(asyncChannel.tryNext() == .element(2))
    #expect(await producer.value == true)
    #expect(asyncChannel.tryNext() == .element(3))
    #expect(asyncChannel.tryNext() == .empty)
}


// polling racing with sends from many producers: every element is received exactly once, in order per producer
@Test func tryNextRace() async {
    let asyncChannel = AsyncChannel<(producer: Int, value: Int)>()
    let sink = asyncChannel.makeSink()
    let producers = 4
    let perProducer = 20_000
    let tasks = (0..<producers).map { producer in
        Task.detached {
            for value in 0..<perProducer {
                sink.send((producer, value))
            }
        }
    }
    let finisher = Task.detached {
        for task in tasks { await task.value }
        sink.finish()
    }
    var expected = [Int](repeating: 0, count: producers)
    loop: while true {
        switch asyncChannel.tryNext() {
        case .element(let next):
            #expect(next.value == expected[next.producer])
            expected[next.producer] += 1
        case .empty:
            await Task.yield()
        case .finished:
            break loop
        }
    }
    #expect(expected == [Int](repeating: perProducer, count: producers))
    await finisher.value
}


// the high watermark switches backpressure on, the low watermark switches it off
@Test func backpressureHysteresis() async {
    let asyncChannel = AsyncChannel<Int>(lowWatermark: 2, highWatermark: 4)
    let sink = asyncChannel.makeSink()
    #expect(sink.send(1) == .sent)
    #expect(sink.send(2) == .sent)
    #expect(sink.send(3) == .sent)
    #expect(sink.send(4) == .backpressure)
    // stays on below the high watermark until the low one is reached
    #expect(await asyncChannel.next() == 1)
    #expect(sink.send(5) == .backpressure)
    #expect(await asyncChannel.next() == 2)
    #expect(await asyncChannel.next() == 3)
    #expect(await sink.waitForDemand() == true)
    #expect(sink.send(6) == .sent)
    #expect(await asyncChannel.next() == 4)
    #expect(await asyncChannel.next() == 5)
    #expect(await asyncChannel.next() == 6)
}


// without watermarks there is no backpressure
@Test func noBackpressureByDefault() async {
    let asyncChannel = AsyncChannel<Int>()
    let sink = asyncChannel.makeSink()
    for i in 0..<1000 {
        #expect(sink.send(i) == .sent)
    }
    #expect(await sink.waitForDemand() == true)
}


// a waiting producer is resumed when the consumer gets to the low watermark
@Test func waitForDemandResumes() async {
    let asyncChannel = AsyncChannel<Int>(lowWatermark: 1, highWatermark: 3)
    let sink = asyncChannel.makeSink()
    sink.send(1)
    sink.send(2)
    #expect(sink.send(3) == .backpressure)
    // counted before next(), the waiter is resumed from inside next()
    let consuming = OSAllocatedUnfairLock(initialState: 0)
    let producer = Task {
        let result = await sink.waitForDemand()
        return (result, consuming.withLock { $0 })
    }
    try? await Task.sleep(for: .milliseconds(50))
    for expected in 1...3 {
        consuming.withLock { $0 += 1 }
        #expect(await asyncChannel.next() == expected)
    }
    let (result, consumingWhenResumed) = await producer.value
    #expect(result == true)
    // resumed when the count went down to 1, that is by the 2nd next()
    #expect(consumingWhenResumed >= 2)
}


@Test func waitForDemandCancelled() async {
    let asyncChannel = AsyncChannel<Int>(lowWatermark: 0, highWatermark: 1)
    let sink = asyncChannel.makeSink()
    #expect(sink.send(1) == .backpressure)
    let producer = Task { await sink.waitForDemand() }
    try? await Task.sleep(for: .milliseconds(50))
    producer.cancel()
    #expect(await producer.value == false)
    // the channel is not closed by that
    #expect(await asyncChannel.next() == 1)
    #expect(await sink.waitForDemand() == true)
    #expect(sink.send(2) == .backpressure)
    #expect(await asyncChannel.next() == 2)
}


@Test func waitForDemandAlreadyCancelled() async {
    let asyncChannel = AsyncChannel<Int>(lowWatermark: 0, highWatermark: 1)
    let sink = asyncChannel.makeSink()
    #expect(sink.send(1) == .backpressure)
    let producer = Task {
        while !Task.isCancelled { await Task.yield() }
        return await sink.waitForDemand()
    }
    producer.cancel()
    #expect(await producer.value == false)
}


@Test func finishResumesWaiters() async {
    let asyncChannel = AsyncChannel<Int>(lowWatermark: 0, highWatermark: 1)
    let sink = asyncChannel.makeSink()
    #expect(sink.send(1) == .backpressure)
    let producers = (0..<4).map { _ in Task { await sink.waitForDemand() } }
    try? await Task.sleep(for: .milliseconds(50))
    sink.finish()
    for producer in producers {
        #expect(await producer.value == false)
    }
    #expect(await sink.waitForDemand() == false)
    #expect(await asyncChannel.next() == 1)
    #expect(await asyncChannel.next() == nil)
}


// dropping the consumer closes the channel and resumes the waiters
@Test func dropConsumerResumesWaiters() async {
    var sink: AsyncChannel<Int>.Sink?
    do {
        let asyncChannel = AsyncChannel<Int>(lowWatermark: 0, highWatermark: 1)
        sink = asyncChannel.makeSink()
    }
    #expect(sink!.send(1) == .closed)
    #expect(await sink!.waitForDemand() == false)

    // the consumer is dropped while the producer waits
    let (ready, readyContinuation) = AsyncStream.makeStream(of: AsyncChannel<Int>.Sink.self)
    let consumer = Task {
        let asyncChannel = AsyncChannel<Int>(lowWatermark: 0, highWatermark: 1)
        readyContinuation.yield(asyncChannel.makeSink())
        try? await Task.sleep(for: .milliseconds(100))
    }
    var iterator = ready.makeAsyncIterator()
    let producerSink = await iterator.next()!
    #expect(producerSink.send(1) == .backpressure)
    #expect(await producerSink.waitForDemand() == false)
    await consumer.value
}


// cancellation racing with the resume: every waiter is resumed exactly once
@Test func waitForDemandCancelRacesResume() async {
    for _ in 0..<2000 {
        let asyncChannel = AsyncChannel<Int>(lowWatermark: 0, highWatermark: 1)
        let sink = asyncChannel.makeSink()
        #expect(sink.send(0) == .backpressure)
        let producer = Task { await sink.waitForDemand() }
        await Task.yield()
        async let consumed = asyncChannel.next()
        producer.cancel()
        _ = await producer.value
        #expect(await consumed == 0)
    }
}


// several producers respecting the backpressure: nothing is lost, the order of
// every producer is kept and the buffer does not grow above high + producers
@Test func backpressureManyProducers() async {
    let producerCount = 4
    let perProducer = 20_000
    let high = 16
    let asyncChannel = AsyncChannel<Int>(lowWatermark: 4, highWatermark: high)
    let sent = OSAllocatedUnfairLock(initialState: 0)
    let waits = OSAllocatedUnfairLock(initialState: 0)
    for producer in 0..<producerCount {
        let sink = asyncChannel.makeSink()
        Task.detached {
            for i in 0..<perProducer {
                let result = sink.send(producer * perProducer + i)
                sent.withLock { $0 += 1 }
                switch result {
                case .sent:
                    break
                case .backpressure:
                    waits.withLock { $0 += 1 }
                    guard await sink.waitForDemand() else { return }
                case .closed:
                    return
                }
            }
        }
    }
    var next = [Int](repeating: 0, count: producerCount)
    var maxBuffered = 0
    for consumed in 0..<(producerCount * perProducer) {
        guard let value = await asyncChannel.next() else {
            Issue.record("finished early")
            return
        }
        let producer = value / perProducer
        #expect(value % perProducer == next[producer])
        next[producer] += 1
        maxBuffered = max(maxBuffered, sent.withLock { $0 } - consumed)
    }
    #expect(next == [Int](repeating: perProducer, count: producerCount))
    #expect(maxBuffered <= high + producerCount)
    #expect(waits.withLock { $0 } > 0)
}
