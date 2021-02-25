// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "hummingbird-redis",
    products: [
        .library(name: "HummingbirdRedis", targets: ["HummingbirdRedis"]),
    ],
    dependencies: [
        .package(url: "https://github.com/adam-fowler/hummingbird.git", from: "0.5.0"),
        .package(url: "https://gitlab.com/mordil/RediStack.git", from: "1.1.0"),
    ],
    targets: [
        .target(name: "HummingbirdRedis", dependencies: [
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "RediStack", package: "RediStack"),
        ]),
        .testTarget(name: "HummingbirdRedisTests", dependencies: [
            .byName(name: "HummingbirdRedis"),
            .product(name: "HummingbirdXCT", package: "hummingbird")
        ]),
    ]
)
