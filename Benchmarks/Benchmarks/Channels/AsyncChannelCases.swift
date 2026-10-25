import Benchmark
import LockFree

func asyncChannel(_ benchmark: Benchmark, _ mode: Mode) async {
    let count = benchmark.scaledIterations.count
    let channel = AsyncChannel<Int>()
    let sink = channel.makeSink()
    let consumer = Task {
        var sum = 0
        for _ in 0..<count {
            sum &+= await channel.next()!
        }
        return sum
    }
    benchmark.startMeasurement()
    for i in benchmark.scaledIterations {
        sink.send(i)
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
