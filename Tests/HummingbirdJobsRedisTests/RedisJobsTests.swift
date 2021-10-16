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
@testable import HummingbirdJobs
@testable import HummingbirdJobsRedis
import HummingbirdXCT
import XCTest

final class HummingbirdRedisJobsTests: XCTestCase {
    static let env = HBEnvironment()
    static let redisHostname = env.get("REDIS_HOSTNAME") ?? "localhost"

    func testBasic() throws {
        struct TestJob: HBJob {
            static let name = "testBasic"
            static let expectation = XCTestExpectation(description: "Jobs Completed")

            let value: Int
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                print(self.value)
                return eventLoop.scheduleTask(in: .milliseconds(Int64.random(in: 10..<50))) {
                    Self.expectation.fulfill()
                }.futureResult
            }
        }
        HBJobRegister.register(job: TestJob.self)
        TestJob.expectation.expectedFulfillmentCount = 10
        TestJob.register()

        let app = HBApplication(testing: .live)
        try app.addRedis(
            configuration: .init(
                hostname: Self.redisHostname,
                port: 6379,
                pool: .init(connectionRetryTimeout: .seconds(1))
            )
        )
        app.logger.logLevel = .trace
        app.addJobs(using: .redis(configuration: .init(queueKey: "testBasic")))

        try app.start()
        defer { app.stop() }

        app.jobs.queue.enqueue(TestJob(value: 1), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 2), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 3), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 4), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 5), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 6), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 7), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 8), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 9), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 0), on: app.eventLoopGroup.next())

        wait(for: [TestJob.expectation], timeout: 5)
    }

    func testMultipleWorkers() throws {
        struct TestJob: HBJob {
            static let name = "testMultipleWorkers"
            static let expectation = XCTestExpectation(description: "Jobs Completed")

            let value: Int
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                print(self.value)
                return eventLoop.scheduleTask(in: .milliseconds(Int64.random(in: 10..<50))) {
                    Self.expectation.fulfill()
                }.futureResult
            }
        }
        HBJobRegister.register(job: TestJob.self)
        TestJob.expectation.expectedFulfillmentCount = 10

        let app = HBApplication(testing: .live)
        try app.addRedis(
            configuration: .init(
                hostname: Self.redisHostname,
                port: 6379,
                pool: .init(connectionRetryTimeout: .seconds(1))
            )
        )
        app.logger.logLevel = .trace
        app.addJobs(using: .redis(configuration: .init(queueKey: "testMultipleWorkers")), numWorkers: 4)
        TestJob.register()

        try app.start()
        defer { app.stop() }

        app.jobs.queue.enqueue(TestJob(value: 1), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 2), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 3), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 4), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 5), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 6), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 7), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 8), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 9), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 0), on: app.eventLoopGroup.next())

        wait(for: [TestJob.expectation], timeout: 5)
    }

    func testIntermitantEnqueue() throws {
        struct TestJob: HBJob {
            static let name = "testBasic"
            static let expectation = XCTestExpectation(description: "Jobs Completed")

            let value: Int
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                print(self.value)
                return eventLoop.scheduleTask(in: .milliseconds(Int64.random(in: 10..<50))) {
                    Self.expectation.fulfill()
                }.futureResult
            }
        }
        HBJobRegister.register(job: TestJob.self)
        TestJob.expectation.expectedFulfillmentCount = 10
        TestJob.register()

        let app = HBApplication(testing: .live)
        try app.addRedis(
            configuration: .init(
                hostname: Self.redisHostname,
                port: 6379,
                pool: .init(connectionRetryTimeout: .seconds(1))
            )
        )
        app.logger.logLevel = .trace
        app.addJobs(using: .redis(configuration: .init(queueKey: "testBasic")))

        try app.start()
        defer { app.stop() }

        app.jobs.queue.enqueue(TestJob(value: 1), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 2), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 3), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 4), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 5), on: app.eventLoopGroup.next())
        // stall to ensure previous jobs have resolve and workers are waiting
        Thread.sleep(forTimeInterval: 0.5)
        app.jobs.queue.enqueue(TestJob(value: 6), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 7), on: app.eventLoopGroup.next())
        // stall to ensure previous jobs have resolve and workers are waiting
        Thread.sleep(forTimeInterval: 0.5)
        app.jobs.queue.enqueue(TestJob(value: 8), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 9), on: app.eventLoopGroup.next())
        app.jobs.queue.enqueue(TestJob(value: 0), on: app.eventLoopGroup.next())

        wait(for: [TestJob.expectation], timeout: 5)
    }

    func testErrorRetryCount() throws {
        struct FailedError: Error {}

        struct TestJob: HBJob {
            static let name = "testErrorRetryCount"
            static let maxRetryCount = 3
            static let expectation = XCTestExpectation(description: "Jobs Completed")
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.expectation.fulfill()
                return eventLoop.makeFailedFuture(FailedError())
            }
        }
        TestJob.expectation.expectedFulfillmentCount = 4
        TestJob.register()

        let app = HBApplication(testing: .live)
        try app.addRedis(
            configuration: .init(
                hostname: Self.redisHostname,
                port: 6379,
                pool: .init(connectionRetryTimeout: .seconds(1))
            )
        )
        app.logger.logLevel = .trace
        app.addJobs(using: .redis(configuration: .init(queueKey: "testErrorRetryCount")))

        try app.start()
        defer { app.stop() }

        app.jobs.queue.enqueue(TestJob(), on: app.eventLoopGroup.next())

        wait(for: [TestJob.expectation], timeout: 1)
    }

    func testSecondQueue() throws {
        struct TestJob: HBJob {
            static let name = "testSecondQueue"
            static let expectation = XCTestExpectation(description: "Jobs Completed")
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.expectation.fulfill()
                return eventLoop.makeSucceededVoidFuture()
            }
        }
        TestJob.expectation.expectedFulfillmentCount = 1
        TestJob.register()

        let app = HBApplication(testing: .live)
        try app.addRedis(
            configuration: .init(
                hostname: Self.redisHostname,
                port: 6379,
                pool: .init(connectionRetryTimeout: .seconds(1))
            )
        )
        app.logger.logLevel = .trace
        app.addJobs(using: .memory)
        app.jobs.registerQueue(.redisTest, queue: .redis(configuration: .init(queueKey: "testSecondQueue")))

        try app.start()
        defer { app.stop() }

        app.jobs.queues(.redisTest).enqueue(TestJob(), on: app.eventLoopGroup.next())

        wait(for: [TestJob.expectation], timeout: 1)
    }

    func testShutdown() throws {
        let app = HBApplication(testing: .live)
        try app.addRedis(
            configuration: .init(
                hostname: Self.redisHostname,
                port: 6379,
                pool: .init(connectionRetryTimeout: .seconds(1))
            )
        )
        app.logger.logLevel = .trace
        app.addJobs(using: .redis(configuration: .init(queueKey: "testBasic")))
        try app.start()
        app.stop()
        app.wait()
    }

    func testShutdownJob() throws {
        class TestJob: HBJob {
            static let name = "testShutdownJob"
            static var started: Bool = false
            static var finished: Bool = false
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.started = true
                let job = eventLoop.scheduleTask(in: .milliseconds(500)) {
                    Self.finished = true
                }
                return job.futureResult
            }
        }
        TestJob.register()

        let app = HBApplication(testing: .live)
        try app.addRedis(
            configuration: .init(
                hostname: Self.redisHostname,
                port: 6379,
                pool: .init(connectionRetryTimeout: .seconds(1))
            )
        )
        app.logger.logLevel = .trace
        app.addJobs(using: .redis(configuration: .init(queueKey: "testShutdownJob")))

        try app.start()

        let job = TestJob()
        app.jobs.queue.enqueue(job, on: app.eventLoopGroup.next())

        // stall to give job chance to start running
        Thread.sleep(forTimeInterval: 0.5)
        app.stop()
        app.wait()

        XCTAssertTrue(TestJob.started)
        XCTAssertTrue(TestJob.finished)
    }
}

extension HBJobQueueId {
    static var redisTest: HBJobQueueId { "test" }
}
