import Benchmark

func asyncStream(_ benchmark: Benchmark, _ mode: Mode) async {
    let count = benchmark.scaledIterations.count
    let (stream, continuation) = AsyncStream<Int>.makeStream(bufferingPolicy: .unbounded)
    let consumer = Task {
        var iterator = stream.makeAsyncIterator()
        var sum = 0
        for _ in 0..<count {
            sum &+= await iterator.next()!
        }
        return sum
    }
    benchmark.startMeasurement()
    for i in benchmark.scaledIterations {
        continuation.yield(i)
    }
    if mode == .dispatch {
        benchmark.stopMeasurement()
    }
    let sum = await consumer.value
    if mode == .endToEnd {
        benchmark.stopMeasurement()
    }
    precondition(sum == expectedSum(count: count))
}
