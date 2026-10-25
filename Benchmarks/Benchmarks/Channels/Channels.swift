import Benchmark

// what one iteration measures, with the consumer running in both cases
enum Mode {
    // time to dispatch all elements
    case dispatch
    // time from the first dispatch until the consumer received the last element
    case endToEnd
}

// every iteration sends `scalingFactor` elements, so the reported metrics are per element
let benchmarks: @Sendable () -> Void = {
    Benchmark.defaultConfiguration = .init(
        metrics: [.wallClock, .cpuTotal, .throughput, .mallocCountTotal, .contextSwitches],
        warmupIterations: 2,
        scalingFactor: .mega,
        maxDuration: .seconds(10),
        maxIterations: 20
    )

    let cases: [(name: String, run: @Sendable (Benchmark, Mode) async -> Void)] = [
        ("AsyncChannel", asyncChannel),
        ("AsyncStream", asyncStream),
        ("MultiProducerSingleConsumerAsyncChannel", mpscChannel),
    ]
    for (name, run) in cases {
        Benchmark("\(name) dispatch") { benchmark in
            await run(benchmark, .dispatch)
        }
        Benchmark("\(name) first dispatch to last receive") { benchmark in
            await run(benchmark, .endToEnd)
        }
    }
}

func expectedSum(count: Int) -> Int {
    count * (count - 1) / 2
}
