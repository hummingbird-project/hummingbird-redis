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
import HummingbirdRedis
import HummingbirdXCT
import NIOPosix
import XCTest

final class HummingbirdRedisTests: XCTestCase {
    static let env = HBEnvironment()
    static let redisHostname = env.get("REDIS_HOSTNAME") ?? "localhost"

    func testApplication() throws {
        let app = HBApplication()
        try app.addRedis(configuration: .init(hostname: Self.redisHostname, port: 6379))

        let info = try app.redis.send(command: "INFO").wait()
        XCTAssertEqual(info.string?.contains("redis_version"), true)
    }

    func testRouteHandler() throws {
        let app = HBApplication(testing: .live)
        try app.addRedis(configuration: .init(hostname: Self.redisHostname, port: 6379))
        app.router.get("redis") { req in
            req.redis.send(command: "INFO").map(\.description)
        }
        try app.XCTStart()
        defer { app.XCTStop() }

        try app.XCTExecute(uri: "/redis", method: .GET) { response in
            var body = try XCTUnwrap(response.body)
            XCTAssertEqual(body.readString(length: body.readableBytes)?.contains("redis_version"), true)
        }
    }

    func testSecondRedis() async throws {
        let app = HBApplication(testing: .live)
        // add dummy default redis which connects to local web server (all we need is a live connection)
        try app.addRedis(configuration: .init(hostname: "localhost", port: 8080))
        try app.addRedis(id: .test, configuration: .init(hostname: Self.redisHostname, port: 6379))
        app.router.put("test/:value") { request -> HTTPResponseStatus in
            let value = try request.parameters.require("value")
            try await request.redis(id: .test).set("Test", to: value).get()
            return .ok
        }
        app.router.get("test") { request -> String? in
            try await request.redis(id: .test).get("Test").get().string
        }
        try app.XCTStart()
        defer { app.XCTStop() }

        let tag = UUID().uuidString
        try app.XCTExecute(uri: "/test/\(tag)", method: .PUT) { response in
            XCTAssertEqual(response.status, .ok)
        }
        try app.XCTExecute(uri: "/test/", method: .GET) { response in
            let body = try XCTUnwrap(response.body)
            XCTAssertEqual(String(buffer: body), tag)
        }
    }

    func testRedisOutsideApplication() async throws {
        let app = HBApplication(testing: .live)
        try app.XCTStart()
        defer { app.XCTStop() }

        let redisConnectionPoolGroup = try RedisConnectionPoolGroup(
            configuration: .init(hostname: Self.redisHostname, port: 6379),
            eventLoopGroup: app.eventLoopGroup,
            logger: app.logger
        )
        let eventLoop = app.eventLoopGroup.any()
        let redis = redisConnectionPoolGroup.pool(for: eventLoop)
        try await redis.set("Test", to: "hello").get()
        let value = try await redis.get("Test").get()
        XCTAssertEqual(value.string, "hello")
    }
}

extension RedisConnectionPoolGroupIdentifier {
    static var test: Self {
        .init(id: "test")
    }
}
