public import CLockFree

@_cdecl("asyncChannelResumeConsumer")
private func asyncChannelResumeConsumer(_ ptr: UnsafeMutableRawPointer?) {
    let continuation = ptr!.assumingMemoryBound(to: CheckedContinuation<Void, Never>?.self)
    guard let continuation = continuation.pointee.take() else {
        fatalError("AsyncChannel: internal error")
    }
    continuation.resume()
}


@usableFromInline
struct ProducerWaiter: @unchecked Sendable {
    @usableFromInline
    let waiter: UnsafeMutableRawPointer
    @usableFromInline
    let continuation: UnsafeMutablePointer<CheckedContinuation<Bool, Never>?>

    @usableFromInline
    init(_ waiter: UnsafeMutableRawPointer) {
        self.waiter = waiter
        self.continuation = waiter.initializeMemory(as: CheckedContinuation<Bool, Never>?.self, to: .none)
    }
}


@_cdecl("asyncChannelResumeProducer")
private func asyncChannelResumeProducer(_ waiter: UnsafeMutableRawPointer?, _ outcome: Int32) {
    let continuation = waiter!.assumingMemoryBound(to: CheckedContinuation<Bool, Never>?.self).move()
    continuation!.resume(returning: outcome == ASYNC_CHANNEL_WAITER_DEMAND)
}


public struct AsyncChannel<Element: Sendable>: ~Copyable {
    @usableFromInline
    let storage: _Storage

    /// Creates a channel without backpressure: `send` never reports it.
    @inlinable
    public init() {
        self.storage = _Storage(lowWatermark: 0, highWatermark: 0)
    }

    /// Creates a channel with backpressure: once `highWatermark` elements are
    /// buffered, `send` returns `.backpressure` until the consumer takes the
    /// count down to `lowWatermark`.
    @inlinable
    public init(lowWatermark: Int, highWatermark: Int) {
        precondition(lowWatermark >= 0 && lowWatermark < highWatermark, "AsyncChannel: 0 <= lowWatermark < highWatermark")
        self.storage = _Storage(lowWatermark: lowWatermark, highWatermark: highWatermark)
    }

    /// Dropping the consumer closes the channel, so producers waiting
    /// for the demand are resumed and the next `send` fails.
    deinit {
        storage.finish()
    }

    @inlinable
    public func next(
        isolation: isolated (any Actor)? = #isolation
    ) async -> Element? {
        await self.storage.next()
    }

    public enum NextResult: Sendable {
        case element(Element)
        /// The channel is finished and drained.
        case finished
        /// Neither an element nor the end of the channel arrived in time.
        case timedOut
    }

    /// Like `next()`, but returns `.timedOut` if no element arrives
    /// and the channel is not finished within `timeout`.
    @inlinable
    public func next(
        timeout: Duration,
        isolation: isolated (any Actor)? = #isolation
    ) async -> NextResult {
        await self.storage.next(deadline: .now.advanced(by: timeout))
    }

    public enum TryNextResult: Sendable {
        case element(Element)
        /// No element is available right now, the channel is still open.
        case empty
        /// The channel is finished and drained.
        case finished
    }

    /// Returns immediately: the next element if one is available,
    /// `.empty` if the channel is open but has nothing to deliver yet
    /// (an element whose `send` is still in progress is not visible),
    /// `.finished` if the channel is finished and drained.
    @inlinable
    public func tryNext() -> TryNextResult {
        self.storage.tryNext()
    }

    @inlinable
    public func makeSink() -> Sink {
        Sink(storage)
    }
}

extension AsyncChannel {
    @usableFromInline
    final class _Storage: @unchecked Sendable {
        @usableFromInline
        let asyncChannel: OpaquePointer

        /// Allocated along with the channel, set while the consumer is parked.
        @usableFromInline
        let consumerContinuation: UnsafeMutablePointer<CheckedContinuation<Void, Never>?>

        @usableFromInline
        init(lowWatermark: Int, highWatermark: Int) {
            var consumerContinuationStorage: UnsafeMutableRawPointer?
            let asyncChannel = async_channel_create(
                MemoryLayout<Element>.stride,
                MemoryLayout<Element>.alignment,
                MemoryLayout<CheckedContinuation<Void, Never>?>.stride,
                &consumerContinuationStorage,
                asyncChannelResumeConsumer,
                UInt64(lowWatermark),
                UInt64(highWatermark),
                asyncChannelResumeProducer
            )
            guard let asyncChannel else {
                fatalError("AsyncChannel: out of memory")
            }
            self.asyncChannel = asyncChannel
            self.consumerContinuation = consumerContinuationStorage!.initializeMemory(as: CheckedContinuation<Void, Never>?.self, to: .none)
        }

        deinit {
            while let ptr = async_channel_next(asyncChannel) {
                if async_channel_is_closed(asyncChannel, ptr) != 0 {
                    break
                }
                ptr.assumingMemoryBound(to: Element.self).deinitialize(count: 1)
            }
            consumerContinuation.deinitialize(count: 1)
            async_channel_destroy(asyncChannel)
        }

        @inlinable
        func next(
            isolation: isolated (any Actor)? = #isolation
        ) async -> Element? {
            while true {
                if let ptr = async_channel_next(asyncChannel) {
                    if async_channel_is_closed(asyncChannel, ptr) != 0 {
                        return nil
                    } else {
                        return ptr.assumingMemoryBound(to: Element.self).move()
                    }
                }
                await withTaskCancellationHandler(
                    operation: {
                        await withCheckedContinuation {
                            consumerContinuation.pointee = $0
                            if async_channel_consumer_try_park(asyncChannel) == 0 {
                                consumerContinuation.pointee = nil
                                $0.resume()
                            }
                        }
                    },
                    onCancel: {
                        async_channel_finish(asyncChannel)
                    }
                )
            }
        }

        @inlinable
        func next(
            deadline: ContinuousClock.Instant,
            isolation: isolated (any Actor)? = #isolation
        ) async -> NextResult {
            while true {
                if let ptr = async_channel_next(asyncChannel) {
                    if async_channel_is_closed(asyncChannel, ptr) != 0 {
                        return .finished
                    } else {
                        return .element(ptr.assumingMemoryBound(to: Element.self).move())
                    }
                }
                if ContinuousClock.now >= deadline {
                    return .timedOut
                }
                var timer: Task<Void, Never>?
                await withTaskCancellationHandler(
                    operation: {
                        await withCheckedContinuation {
                            consumerContinuation.pointee = $0
                            if async_channel_consumer_try_park(asyncChannel) == 0 {
                                consumerContinuation.pointee = nil
                                $0.resume()
                                return
                            }
                            // started only once parked, so it sees the parked slot
                            timer = Task.detached { [self] in
                                let result: Void? = try? await ContinuousClock().sleep(until: deadline)
                                if result != nil {
                                    // timed out
                                    if async_channel_consumer_try_unpark(self.asyncChannel) != 0 {
                                        self.consumerContinuation.pointee.take()!.resume()
                                    }
                                }
                            }
                        }
                    },
                    onCancel: {
                        async_channel_finish(asyncChannel)
                    }
                )
                // the timer touches the parked slot, so it has to be done
                // before the consumer moves on and the block can be freed
                if let timer {
                    timer.cancel()
                    await timer.value
                }
            }
        }

        @inlinable
        func tryNext() -> TryNextResult {
            guard let ptr = async_channel_next(asyncChannel) else {
                return .empty
            }
            if async_channel_is_closed(asyncChannel, ptr) != 0 {
                return .finished
            } else {
                return .element(ptr.assumingMemoryBound(to: Element.self).move())
            }
        }

        @inlinable
        func send(_ element: Element) -> SendResult {
            var position: UInt64 = 0
            guard let ptr = async_channel_reserve(asyncChannel, &position) else {
                return .closed
            }
            assert(UInt(bitPattern: ptr) & UInt(MemoryLayout<Element>.alignment - 1) == 0)
            ptr.initializeMemory(as: Element.self, to: element)
            if async_channel_send(asyncChannel, ptr, position) != 0 {
                return .backpressure
            } else {
                return .sent
            }
        }

        @inlinable
        func waitForDemand(
            isolation: isolated (any Actor)? = #isolation
        ) async -> Bool {
            switch async_channel_demand(asyncChannel) {
            case ASYNC_CHANNEL_WAIT_DEMAND:
                return true
            case ASYNC_CHANNEL_WAIT_CLOSED:
                return false
            default:
                break
            }
            guard let waiter = async_channel_producer_waiter_create(MemoryLayout<CheckedContinuation<Bool, Never>?>.stride) else {
                fatalError("AsyncChannel: out of memory")
            }
            let producerWaiter = ProducerWaiter(waiter)
            let result = await withTaskCancellationHandler(
                operation: {
                    await withCheckedContinuation {
                        producerWaiter.continuation.pointee = $0
                        let result = async_channel_producer_try_park(asyncChannel, waiter)
                        if result == ASYNC_CHANNEL_WAIT_QUEUED || result == ASYNC_CHANNEL_WAIT_RACED {
                            // the continuation is owned by whoever resumes it
                            return
                        }
                        producerWaiter.continuation.deinitialize(count: 1)
                        switch result {
                        case ASYNC_CHANNEL_WAIT_DEMAND:
                            $0.resume(returning: true)
                        case ASYNC_CHANNEL_WAIT_CLOSED, ASYNC_CHANNEL_WAIT_CANCELLED:
                            $0.resume(returning: false)
                        default:
                            fatalError("Internal error: unexpected async_channel_producer_try_park() result \(result)")
                        }
                    }
                },
                onCancel: {
                    if async_channel_producer_waiter_cancel(producerWaiter.waiter) != 0 {
                        guard let continuation = producerWaiter.continuation.move() else {
                            fatalError("Internal error: continuation must not be empty")
                        }
                        continuation.resume(returning: false)
                    }
                }
            )
            async_channel_producer_waiter_release(waiter)
            return result
        }

        @inlinable
        func finish() {
            async_channel_finish(asyncChannel)
        }
    }
}

extension AsyncChannel {
    public enum SendResult: Sendable, Hashable {
        /// The element is sent, more elements can be sent.
        case sent
        /// The element is sent, but the high watermark is reached:
        /// call `waitForDemand()` before sending more.
        case backpressure
        /// The channel is closed, the element is not sent.
        case closed
    }

    public struct Sink: Sendable {
        @usableFromInline
        let storage: _Storage

        @inlinable
        init(_ storage: _Storage) {
            self.storage = storage
        }

        @inlinable
        @discardableResult
        public func send(_ element: Element) -> SendResult {
            storage.send(element)
        }

        /// Suspends while the channel is backpressured, that is from the moment
        /// the high watermark is reached until the consumer takes the number of
        /// the buffered elements down to the low watermark.
        ///
        /// Returns `true` if more elements should be sent, `false` if the
        /// channel is closed or the task is cancelled.
        @inlinable
        public func waitForDemand(
            isolation: isolated (any Actor)? = #isolation
        ) async -> Bool {
            await storage.waitForDemand()
        }

        /// Closes the channel: the elements sent so far are still delivered,
        /// then `next()` returns `nil`.
        @inlinable
        public func finish() {
            storage.finish()
        }
    }
}

extension AsyncChannel.NextResult: Equatable where Element: Equatable {}
extension AsyncChannel.NextResult: Hashable where Element: Hashable {}
extension AsyncChannel.TryNextResult: Equatable where Element: Equatable {}
extension AsyncChannel.TryNextResult: Hashable where Element: Hashable {}
