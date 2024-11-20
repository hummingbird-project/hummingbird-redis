// swift-tools-version:5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "hummingbird-redis",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "HummingbirdRedis", targets: ["HummingbirdRedis"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.5.0"),
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.4.0"),
    ],
    targets: [
        .target(name: "HummingbirdRedis", dependencies: [
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "RediStack", package: "RediStack"),
        ]),
        .testTarget(name: "HummingbirdRedisTests", dependencies: [
            .byName(name: "HummingbirdRedis"),
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "HummingbirdTesting", package: "hummingbird"),
        ]),
    ]
)
