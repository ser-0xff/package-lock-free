// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "package-lock-free",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
    ],
    products: [
        .library(
            name: "LockFree",
            targets: ["LockFree"]
        ),
    ],
    dependencies: [
    ],
    targets: [
        .target(
            name: "LockFree",
            dependencies: [
                "CLockFree"
            ]
        ),
        .target(
            name: "CLockFree"
        ),
        .testTarget(
            name: "LockFreeTests",
            dependencies: ["LockFree", "CLockFree"]
        )
    ]
)

