// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "benchmarks",
    platforms: [
        .macOS(.v15),
    ],
    dependencies: [
        .package(path: "../"),
        .package(url: "https://github.com/apple/swift-async-algorithms.git", from: "1.1.7"),
    ],
    targets: [
        .executableTarget(
            name: "Benchmarks",
            dependencies: [
                .product(name: "LockFree", package: "package-lock-free"),
                .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
            ]
        ),
    ]
)

