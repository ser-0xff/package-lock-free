struct Measurement {
    let name: String
    var samples: [Duration] = []

    init(_ name: String) {
        self.name = name
    }
}

func expectedSum(count: Int) -> Int {
    count * (count - 1) / 2
}

private func milliseconds(_ duration: Duration) -> Double {
    let components = duration.components
    return Double(components.seconds) * 1e3 + Double(components.attoseconds) / 1e15
}

private func pad(_ string: String, _ width: Int, left: Bool = false) -> String {
    let padding = String(repeating: " ", count: max(0, width - string.count))
    return left ? string + padding : padding + string
}

private func format(_ value: Double) -> String {
    let scaled = (value * 100).rounded() / 100
    let whole = Int(scaled)
    let fraction = Int(((scaled - Double(whole)) * 100).rounded())
    return "\(whole).\(fraction < 10 ? "0" : "")\(fraction)"
}

func report(_ title: String, _ measurements: [Measurement], count: Int) {
    let nameWidth = measurements.map(\.name.count).max() ?? 0
    print()
    print(title)
    print(
        pad("", nameWidth, left: true),
        pad("min ms", 10), pad("median ms", 10), pad("max ms", 10), pad("ns/element", 11)
    )
    for measurement in measurements {
        let sorted = measurement.samples.map(milliseconds).sorted()
        let median = sorted[sorted.count / 2]
        print(
            pad(measurement.name, nameWidth, left: true),
            pad(format(sorted.first!), 10),
            pad(format(median), 10),
            pad(format(sorted.last!), 10),
            pad(format(median * 1e6 / Double(count)), 11)
        )
    }
}
