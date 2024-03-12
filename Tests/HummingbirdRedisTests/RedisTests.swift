//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Hummingbird
@testable import HummingbirdRedis
import HummingbirdTesting
import Logging
import NIOPosix
import XCTest

final class HummingbirdRedisTests: XCTestCase {
    static let env = Environment()
    static let redisHostname = env.get("REDIS_HOSTNAME") ?? "localhost"

    func testApplication() async throws {
        let redis = try RedisConnectionPoolService(
            .init(hostname: Self.redisHostname, port: 6379),
            logger: Logger(label: "Redis")
        )

        let info = try await redis.send(command: "INFO").get()
        XCTAssertEqual(info.string?.contains("redis_version"), true)

        try await redis.close()
    }

    func testRouteHandler() async throws {
        let redis = try RedisConnectionPoolService(
            .init(hostname: Self.redisHostname, port: 6379),
            logger: Logger(label: "Redis")
        )
        let router = Router()
        router.get("redis") { _, _ in
            try await redis.send(command: "INFO").map(\.description).get()
        }
        var app = Application(responder: router.buildResponder())
        app.addServices(redis)
        try await app.test(.live) { client in
            try await client.execute(uri: "/redis", method: .get) { response in
                var body = try XCTUnwrap(response.body)
                XCTAssertEqual(body.readString(length: body.readableBytes)?.contains("redis_version"), true)
            }
        }
    }
}
