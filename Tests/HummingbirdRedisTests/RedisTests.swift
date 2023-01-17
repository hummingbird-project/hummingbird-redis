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
import HummingbirdXCT
import RediStack
import XCTest

extension RedisCommand {
    public static func info() -> RedisCommand<String> {
        return .init(keyword: "INFO", arguments: [])
    }
}

final class HummingbirdRedisTests: XCTestCase {
    static let env = HBEnvironment()
    static let redisHostname = env.get("REDIS_HOSTNAME") ?? "localhost"

    func testApplication() throws {
        let app = HBApplication()
        try app.addRedis(configuration: .init(hostname: Self.redisHostname, port: 6379))

        let info = try app.redis.send(.info(), eventLoop: nil, logger: nil).wait()
        XCTAssertEqual(info.contains("redis_version"), true)
    }

    func testRouteHandler() throws {
        let app = HBApplication(testing: .live)
        try app.addRedis(configuration: .init(hostname: Self.redisHostname, port: 6379))
        app.router.get("redis") { req in
            req.redis.send(.info(), eventLoop: nil, logger: nil).map(\.description)
        }
        try app.XCTStart()
        defer { app.XCTStop() }

        try app.XCTExecute(uri: "/redis", method: .GET) { response in
            var body = try XCTUnwrap(response.body)
            XCTAssertEqual(body.readString(length: body.readableBytes)?.contains("redis_version"), true)
        }
    }
}
