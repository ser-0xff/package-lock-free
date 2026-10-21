import Foundation

let warmUpRounds = 2
let measuredRounds = 8

struct BenchmarkCase: Sendable {
    let key: String
    let name: String
    let run: @Sendable (Int) async -> (dispatch: Duration, endToEnd: Duration)
}

let benchmarkCases = [
    BenchmarkCase(key: "async-channel", name: "AsyncChannel", run: asyncChannelEndToEnd),
    BenchmarkCase(key: "async-stream", name: "AsyncStream", run: asyncStreamEndToEnd),
    BenchmarkCase(key: "mpsc", name: "MultiProducerSingleConsumerAsyncChannel", run: mpscChannelEndToEnd),
]

let usage = """
    usage: ChannelBenchmarks [count] [--only <case>]...
      count   number of elements to send (default 1000000)
      --only  run only the given case, can be repeated: \
    \(benchmarkCases.map(\.key).joined(separator: ", "))
    """

func fail(_ message: String) -> Never {
    print(message)
    print(usage)
    exit(1)
}

@main
struct Main {
    static func main() async {
        var count = 1_000_000
        var only: [String] = []
        var arguments = CommandLine.arguments.dropFirst()
        while let argument = arguments.popFirst() {
            switch argument {
            case "--only":
                guard let key = arguments.popFirst() else {
                    fail("--only needs a case name")
                }
                guard benchmarkCases.contains(where: { $0.key == key }) else {
                    fail("unknown case '\(key)'")
                }
                only.append(key)
            case "--help", "-h":
                print(usage)
                return
            default:
                guard let value = Int(argument), value > 0 else {
                    fail("unexpected argument '\(argument)'")
                }
                count = value
            }
        }
        let cases = only.isEmpty ? benchmarkCases : benchmarkCases.filter { only.contains($0.key) }
        print("elements: \(count), rounds: \(measuredRounds) (after \(warmUpRounds) warm-up)")

        // with the consumer running: time to dispatch all elements,
        // and time from the first dispatch until the consumer received the last element
        var dispatch = cases.map { Measurement($0.name) }
        var endToEnd = cases.map { Measurement($0.name) }
        for round in 0..<(warmUpRounds + measuredRounds) {
            for (index, benchmarkCase) in cases.enumerated() {
                let sample = await benchmarkCase.run(count)
                if round >= warmUpRounds {
                    dispatch[index].samples.append(sample.dispatch)
                    endToEnd[index].samples.append(sample.endToEnd)
                }
            }
        }
        report("dispatch", dispatch, count: count)
        report("first dispatch to last receive", endToEnd, count: count)
    }
}
