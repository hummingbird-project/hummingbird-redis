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
import Logging
import RediStack
import XCTest

final class PersistTests: XCTestCase {
    static let redisHostname = HBEnvironment.shared.get("REDIS_HOSTNAME") ?? "localhost"

    func createRouter() throws -> (HBRouter<HBTestRouterContext>, HBPersistDriver) {
        let router = HBRouter(context: HBTestRouterContext.self)
        let redisConnectionPool = try RedisConnectionPool(
            .init(hostname: Self.redisHostname, port: 6379),
            logger: Logger(label: "Redis")
        )
        let redisConnectionPoolService = RedisConnectionPoolService(pool: redisConnectionPool)
        let persist = HBRedisPersistDriver(redisConnectionPoolService: redisConnectionPoolService)

        router.put("/persist/:tag") { request, context -> HTTPResponse.Status in
            let buffer = try await request.body.collect(upTo: .max)
            let tag = try context.parameters.require("tag")
            try await persist.set(key: tag, value: String(buffer: buffer))
            return .ok
        }
        router.put("/persist/:tag/:time") { request, context -> HTTPResponse.Status in
            guard let time = context.parameters.get("time", as: Int.self) else { throw HBHTTPError(.badRequest) }
            let buffer = try await request.body.collect(upTo: .max)
            let tag = try context.parameters.require("tag")
            try await persist.set(key: tag, value: String(buffer: buffer), expires: .seconds(time))
            return .ok
        }
        router.get("/persist/:tag") { _, context -> String? in
            guard let tag = context.parameters.get("tag", as: String.self) else { throw HBHTTPError(.badRequest) }
            return try await persist.get(key: tag, as: String.self)
        }
        router.delete("/persist/:tag") { _, context -> HTTPResponse.Status in
            guard let tag = context.parameters.get("tag", as: String.self) else { throw HBHTTPError(.badRequest) }
            try await persist.remove(key: tag)
            return .noContent
        }
        return (router, persist)
    }

    func testSetGet() async throws {
        let (router, _) = try createRouter()
        let app = HBApplication(responder: router.buildResponder())
        try await app.test(.router) { client in
            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .get) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "Persist")
            }
        }
    }

    func testCreateGet() async throws {
        let (router, persist) = try createRouter()

        router.put("/create/:tag") { request, context -> HTTPResponse.Status in
            let buffer = try await request.body.collect(upTo: .max)
            let tag = try context.parameters.require("tag")
            try await persist.create(key: tag, value: String(buffer: buffer))
            return .ok
        }
        let app = HBApplication(responder: router.buildResponder())
        try await app.test(.router) { client in
            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/create/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .get) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "Persist")
            }
        }
    }

    func testDoubleCreateFail() async throws {
        let (router, persist) = try createRouter()
        router.put("/create/:tag") { request, context -> HTTPResponse.Status in
            let buffer = try await request.body.collect(upTo: .max)
            let tag = try context.parameters.require("tag")
            do {
                try await persist.create(key: tag, value: String(buffer: buffer))
            } catch let error as HBPersistError where error == .duplicate {
                throw HBHTTPError(.conflict)
            }
            return .ok
        }
        let app = HBApplication(responder: router.buildResponder())
        try await app.test(.router) { client in
            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/create/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "Persist")) { response in
                XCTAssertEqual(response.status, .ok)
            }
            try await client.XCTExecute(uri: "/create/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "Persist")) { response in
                XCTAssertEqual(response.status, .conflict)
            }
        }
    }

    func testSetTwice() async throws {
        let (router, _) = try createRouter()
        let app = HBApplication(responder: router.buildResponder())
        try await app.test(.router) { client in

            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "test1")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "test2")) { response in
                XCTAssertEqual(response.status, .ok)
            }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .get) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "test2")
            }
        }
    }

    func testExpires() async throws {
        let (router, _) = try createRouter()
        let app = HBApplication(responder: router.buildResponder())
        try await app.test(.router) { client in

            let tag1 = UUID().uuidString
            let tag2 = UUID().uuidString

            try await client.XCTExecute(uri: "/persist/\(tag1)/0", method: .put, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag2)/10", method: .put, body: ByteBufferAllocator().buffer(string: "ThisIsTest2")) { _ in }
            try await Task.sleep(nanoseconds: 1_000_000_000)
            try await client.XCTExecute(uri: "/persist/\(tag1)", method: .get) { response in
                XCTAssertEqual(response.status, .noContent)
            }
            try await client.XCTExecute(uri: "/persist/\(tag2)", method: .get) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "ThisIsTest2")
            }
        }
    }

    func testCodable() async throws {
        #if os(macOS)
        // disable macOS tests in CI. GH Actions are currently running this when they shouldn't
        guard HBEnvironment().get("CI") != "true" else { throw XCTSkip() }
        #endif
        struct TestCodable: Codable {
            let buffer: String
        }
        let (router, persist) = try createRouter()
        router.put("/codable/:tag") { request, context -> HTTPResponse.Status in
            guard let tag = context.parameters.get("tag") else { throw HBHTTPError(.badRequest) }
            let buffer = try await request.body.collect(upTo: .max)
            try await persist.set(key: tag, value: TestCodable(buffer: String(buffer: buffer)))
            return .ok
        }
        router.get("/codable/:tag") { _, context -> String? in
            guard let tag = context.parameters.get("tag") else { throw HBHTTPError(.badRequest) }
            let value = try await persist.get(key: tag, as: TestCodable.self)
            return value?.buffer
        }
        let app = HBApplication(responder: router.buildResponder())

        try await app.test(.router) { client in

            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/codable/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
            try await client.XCTExecute(uri: "/codable/\(tag)", method: .get) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "Persist")
            }
        }
    }

    func testRemove() async throws {
        let (router, _) = try createRouter()
        let app = HBApplication(responder: router.buildResponder())
        try await app.test(.router) { client in
            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .put, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .delete) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .get) { response in
                XCTAssertEqual(response.status, .noContent)
            }
        }
    }

    func testExpireAndAdd() async throws {
        let (router, _) = try createRouter()
        let app = HBApplication(responder: router.buildResponder())
        try await app.test(.router) { client in

            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)/0", method: .put, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
            try await Task.sleep(nanoseconds: 1_000_000_000)
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .get) { response in
                XCTAssertEqual(response.status, .noContent)
            }
            try await client.XCTExecute(uri: "/persist/\(tag)/10", method: .put, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { response in
                XCTAssertEqual(response.status, .ok)
            }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .get) { response in
                XCTAssertEqual(response.status, .ok)
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "ThisIsTest1")
            }
        }
    }
    /*        let app = HBApplication(testing: .live)
         // add Fluent
         try app.addRedis(configuration: .init(hostname: Self.redisHostname, port: 6379))
         // add persist
         app.addPersist(using: .redis)

         app.router.put("/persist/:tag") { request -> EventLoopFuture<HTTPResponseStatus> in
             guard let tag = request.parameters.get("tag") else { return request.failure(.badRequest) }
             guard let buffer = request.body.buffer else { return request.failure(.badRequest) }
             return request.persist.set(key: tag, value: String(buffer: buffer))
                 .map { _ in .ok }
         }
         app.router.put("/persist/:tag/:time") { request -> EventLoopFuture<HTTPResponseStatus> in
             guard let time = request.parameters.get("time", as: Int.self) else { return request.failure(.badRequest) }
             guard let tag = request.parameters.get("tag") else { return request.failure(.badRequest) }
             guard let buffer = request.body.buffer else { return request.failure(.badRequest) }
             return request.persist.set(key: tag, value: String(buffer: buffer), expires: .seconds(numericCast(time)))
                 .map { _ in .ok }
         }
         app.router.get("/persist/:tag") { request -> EventLoopFuture<String?> in
             guard let tag = request.parameters.get("tag", as: String.self) else { return request.failure(.badRequest) }
             return request.persist.get(key: tag, as: String.self)
         }
         app.router.delete("/persist/:tag") { request -> EventLoopFuture<HTTPResponseStatus> in
             guard let tag = request.parameters.get("tag", as: String.self) else { return request.failure(.badRequest) }
             return request.persist.remove(key: tag)
                 .map { _ in .noContent }
         }
         return app
     }

     func testSetGet() throws {
         let app = try createApplication()
         try app.XCTStart()
         defer { app.XCTStop() }
         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
         try app.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
             let body = try XCTUnwrap(response.body)
             XCTAssertEqual(String(buffer: body), "Persist")
         }
     }

     func testDoubleCreateFail() throws {
         let app = try createApplication()
         app.router.put("/create/:tag") { request -> EventLoopFuture<HTTPResponseStatus> in
             guard let tag = request.parameters.get("tag") else { return request.failure(.badRequest) }
             guard let buffer = request.body.buffer else { return request.failure(.badRequest) }
             return request.persist.create(key: tag, value: String(buffer: buffer))
                 .flatMapErrorThrowing { error in
                     if let error = error as? HBPersistError, error == .duplicate { throw HBHTTPError(.conflict) }
                     throw error
                 }
                 .map { _ in .ok }
         }
         try app.XCTStart()
         defer { app.XCTStop() }
         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/create/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { response in
             XCTAssertEqual(response.status, .ok)
         }
         try app.XCTExecute(uri: "/create/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { response in
             XCTAssertEqual(response.status, .conflict)
         }
     }

     func testSetTwice() throws {
         let app = try createApplication()
         try app.XCTStart()
         defer { app.XCTStop() }

         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "test1")) { _ in }
         try app.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "test2")) { response in
             XCTAssertEqual(response.status, .ok)
         }
         try app.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
             let body = try XCTUnwrap(response.body)
             XCTAssertEqual(String(buffer: body), "test2")
         }
     }

     func testExpires() throws {
         let app = try createApplication()
         try app.XCTStart()
         defer { app.XCTStop() }

         let tag1 = UUID().uuidString
         let tag2 = UUID().uuidString

         try app.XCTExecute(uri: "/persist/\(tag1)/0", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
         try app.XCTExecute(uri: "/persist/\(tag2)/10", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest2")) { _ in }
         Thread.sleep(forTimeInterval: 1)
         try app.XCTExecute(uri: "/persist/\(tag1)", method: .GET) { response in
             XCTAssertEqual(response.status, .noContent)
         }
         try app.XCTExecute(uri: "/persist/\(tag2)", method: .GET) { response in
             let body = try XCTUnwrap(response.body)
             XCTAssertEqual(String(buffer: body), "ThisIsTest2")
         }
     }

     func testCodable() throws {
         struct TestCodable: Codable {
             let buffer: String
         }
         let app = try createApplication()

         app.router.put("/codable/:tag") { request -> EventLoopFuture<HTTPResponseStatus> in
             guard let tag = request.parameters.get("tag") else { return request.failure(.badRequest) }
             guard let buffer = request.body.buffer else { return request.failure(.badRequest) }
             return request.persist.set(key: tag, value: TestCodable(buffer: String(buffer: buffer)))
                 .map { _ in .ok }
         }
         app.router.get("/codable/:tag") { request -> EventLoopFuture<String?> in
             guard let tag = request.parameters.get("tag") else { return request.failure(.badRequest) }
             return request.persist.get(key: tag, as: TestCodable.self).map { $0.map(\.buffer) }
         }
         try app.XCTStart()
         defer { app.XCTStop() }

         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/codable/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
         try app.XCTExecute(uri: "/codable/\(tag)", method: .GET) { response in
             let body = try XCTUnwrap(response.body)
             XCTAssertEqual(String(buffer: body), "Persist")
         }
     }

     func testRemove() throws {
         let app = try createApplication()
         try app.XCTStart()
         defer { app.XCTStop() }

         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
         try app.XCTExecute(uri: "/persist/\(tag)", method: .DELETE) { _ in }
         try app.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
             XCTAssertEqual(response.status, .noContent)
         }
     }

     func testExpireAndAdd() throws {
         let app = try createApplication()
         try app.XCTStart()
         defer { app.XCTStop() }

         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/persist/\(tag)/0", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
         Thread.sleep(forTimeInterval: 1)
         try app.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
             XCTAssertEqual(response.status, .noContent)
         }
         try app.XCTExecute(uri: "/persist/\(tag)/10", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { response in
             XCTAssertEqual(response.status, .ok)
         }
         try app.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
             XCTAssertEqual(response.status, .ok)
             let body = try XCTUnwrap(response.body)
             XCTAssertEqual(String(buffer: body), "ThisIsTest1")
         }
     }

     func testSecondRedis() throws {
         let app = HBApplication(testing: .live)
         // add dummy default redis which connects to local web server (all we need is a live connection)
         try app.addRedis(configuration: .init(hostname: "localhost", port: 8080))
         try app.addRedis(id: .test, configuration: .init(hostname: Self.redisHostname, port: 6379))
         app.addPersist(using: .redis(id: .test))

         app.router.put("/persist/:tag") { request -> HTTPResponseStatus in
             let tag = try request.parameters.require("tag")
             guard let buffer = request.body.buffer else { throw HBHTTPError(.badRequest) }
             try await request.persist.set(key: tag, value: String(buffer: buffer))
             return .ok
         }
         app.router.get("/persist/:tag") { request -> String? in
             let tag = try request.parameters.require("tag")
             return try await request.persist.get(key: tag, as: String.self)
         }

         try app.XCTStart()
         defer { app.XCTStop() }
         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
         try app.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
             let body = try XCTUnwrap(response.body)
             XCTAssertEqual(String(buffer: body), "Persist")
         }
     }

     func testPersistOutsideApplication() async throws {
         let app = try createApplication()

         let redisConnectionPoolGroup = try RedisConnectionPoolGroup(
             configuration: .init(hostname: Self.redisHostname, port: 6379),
             eventLoopGroup: app.eventLoopGroup,
             logger: app.logger
         )
         let persist = HBRedisPersistDriver(redisConnectionPoolGroup: redisConnectionPoolGroup)

         app.router.put("test/:value") { request -> HTTPResponseStatus in
             let value = try request.parameters.require("value")
             try await persist.set(key: "test", value: value, expires: nil, request: request).get()
             return .ok
         }
         app.router.get("test") { request -> String? in
             try await persist.get(key: "test", as: String.self, request: request).get()
         }
         try app.XCTStart()
         defer { app.XCTStop() }

         let tag = UUID().uuidString
         try app.XCTExecute(uri: "/test/\(tag)", method: .PUT) { _ in }
         try app.XCTExecute(uri: "/test/", method: .GET) { response in
             let body = try XCTUnwrap(response.body)
             XCTAssertEqual(String(buffer: body), tag)
         }
     }*/
}
