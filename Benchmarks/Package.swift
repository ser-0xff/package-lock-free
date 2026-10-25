// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "benchmarks",
    platforms: [
        .macOS(.v15),
    ],
    dependencies: [
        .package(name: "package-lock-free", path: "../"),
        .package(url: "https://github.com/ordo-one/benchmark.git", from: "1.38.0"),
        .package(url: "https://github.com/apple/swift-async-algorithms.git", from: "1.1.7"),
    ],
    targets: [
        .executableTarget(
            name: "Channels",
            dependencies: [
                .product(name: "Benchmark", package: "benchmark"),
                .product(name: "LockFree", package: "package-lock-free"),
                .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
            ],
            path: "Benchmarks/Channels",
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "benchmark"),
            ]
        ),
    ]
)
