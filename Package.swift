// swift-tools-version:6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "hummingbird-valkey",
    platforms: [.macOS(.v15), .iOS(.v18), .tvOS(.v18)],
    products: [
        .library(name: "HummingbirdValkey", targets: ["HummingbirdValkey"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.5.0"),
        .package(url: "https://github.com/valkey-io/valkey-swift.git", from: "0.1.0"),
    ],
    targets: [
        .target(
            name: "HummingbirdValkey",
            dependencies: [
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "Valkey", package: "valkey-swift"),
            ]
        ),
        .testTarget(
            name: "HummingbirdValkeyTests",
            dependencies: [
                .byName(name: "HummingbirdValkey"),
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "HummingbirdTesting", package: "hummingbird"),
            ]
        ),
    ]
)
