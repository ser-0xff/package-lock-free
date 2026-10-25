import AsyncAlgorithms
import Benchmark

// the watermark is above the element count, so the channel never asks the producer to stop
private func makeChannel(
    count: Int
) -> MultiProducerSingleConsumerAsyncChannel<Int, Never>.ChannelAndStream {
    MultiProducerSingleConsumerAsyncChannel.makeChannel(
        of: Int.self,
        backpressureStrategy: .watermark(low: count, high: 2 * count)
    )
}

// synchronous, so the synchronous `send` is picked instead of the one that awaits backpressure
private func send(
    _ elements: Range<Int>,
    to source: inout MultiProducerSingleConsumerAsyncChannel<Int, Never>.Source
) {
    for i in elements {
        _ = try! source.send(i)
    }
}

func mpscChannel(_ benchmark: Benchmark, _ mode: Mode) async {
    let count = benchmark.scaledIterations.count
    var channelAndSource = makeChannel(count: count)
    var channel = channelAndSource.takeChannel()
    var source = consume channelAndSource.source
    let consumer = Task {
        var sum = 0
        for _ in 0..<count {
            sum &+= await channel.next()!
        }
        return sum
    }
    benchmark.startMeasurement()
    send(benchmark.scaledIterations, to: &source)
    if mode == .dispatch {
        benchmark.stopMeasurement()
    }
    let sum = await consumer.value
    if mode == .endToEnd {
        benchmark.stopMeasurement()
    }
    precondition(sum == expectedSum(count: count))
}
