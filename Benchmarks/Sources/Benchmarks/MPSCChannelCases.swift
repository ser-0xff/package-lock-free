import AsyncAlgorithms

// the watermark is above the element count, so the channel never asks the producer to stop
private func makeChannel(
    count: Int
) -> MultiProducerSingleConsumerAsyncChannel<Int, Never>.ChannelAndStream {
    MultiProducerSingleConsumerAsyncChannel.makeChannel(
        of: Int.self,
        backpressureStrategy: .watermark(low: count, high: 2 * count)
    )
}

func mpscChannelEndToEnd(count: Int) async -> (dispatch: Duration, endToEnd: Duration) {
    var channelAndSource = makeChannel(count: count)
    var channel = channelAndSource.takeChannel()
    var source = consume channelAndSource.source
    let consumer = Task {
        var sum = 0
        for _ in 0..<count {
            sum &+= await channel.next()!
        }
        return (end: ContinuousClock.now, sum: sum)
    }
    let producer = Task {
        let start = ContinuousClock.now
        for i in 0..<count {
            _ = try! source.send(i)
        }
        return (start: start, end: ContinuousClock.now)
    }
    let produced = await producer.value
    let consumed = await consumer.value
    precondition(consumed.sum == expectedSum(count: count))
    return (dispatch: produced.end - produced.start, endToEnd: consumed.end - produced.start)
}
