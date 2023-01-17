// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "hummingbird-redis",
    platforms: [.iOS(.v12), .tvOS(.v12)],
    products: [
        .library(name: "HummingbirdRedis", targets: ["HummingbirdRedis"]),
        .library(name: "HummingbirdJobsRedis", targets: ["HummingbirdJobsRedis"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "1.0.0-rc"),
        .package(url: "https://gitlab.com/mordil/RediStack.git", from: "1.1.0"),
    ],
    targets: [
        .target(name: "HummingbirdRedis", dependencies: [
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "RediStack", package: "RediStack"),
        ]),
        .target(name: "HummingbirdJobsRedis", dependencies: [
            .byName(name: "HummingbirdRedis"),
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "HummingbirdJobs", package: "hummingbird"),
            .product(name: "RediStack", package: "RediStack"),
        ]),
        .testTarget(name: "HummingbirdRedisTests", dependencies: [
            .byName(name: "HummingbirdRedis"),
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "HummingbirdXCT", package: "hummingbird"),
        ]),
        .testTarget(name: "HummingbirdJobsRedisTests", dependencies: [
            .byName(name: "HummingbirdJobsRedis"),
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "HummingbirdJobs", package: "hummingbird"),
            .product(name: "HummingbirdXCT", package: "hummingbird"),
        ]),
    ]
)
