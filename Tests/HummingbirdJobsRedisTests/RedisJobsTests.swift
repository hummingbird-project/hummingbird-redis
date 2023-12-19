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

import Atomics
import Hummingbird
import HummingbirdJobs
@testable import HummingbirdJobsRedis
import HummingbirdRedis
import HummingbirdXCT
import RediStack
import ServiceLifecycle
import XCTest

extension XCTestExpectation {
    convenience init(description: String, expectedFulfillmentCount: Int) {
        self.init(description: description)
        self.expectedFulfillmentCount = expectedFulfillmentCount
    }
}

final class HummingbirdRedisJobsTests: XCTestCase {
    func wait(for expectations: [XCTestExpectation], timeout: TimeInterval) async {
        #if (os(Linux) && swift(<5.10)) || swift(<5.8)
        super.wait(for: expectations, timeout: timeout)
        #else
        await fulfillment(of: expectations, timeout: timeout)
        #endif
    }

    static let env = HBEnvironment()
    static let redisHostname = env.get("REDIS_HOSTNAME") ?? "localhost"

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        redis: RedisConnectionPoolService? = nil,
        numWorkers: Int,
        failedJobsInitialization: HBRedisJobQueue.JobInitialization = .remove,
        test: (HBRedisJobQueue) async throws -> T
    ) async throws -> T {
        let redisService: RedisConnectionPoolService
        var additionalServices: [any Service] = []
        if let redis = redis {
            redisService = redis
        } else {
            let redisConnectionPool = try RedisConnectionPool(
                .init(hostname: Self.redisHostname, port: 6379),
                logger: Logger(label: "Redis")
            )
            redisService = RedisConnectionPoolService(pool: redisConnectionPool)
            additionalServices = [redisService]
        }
        let redisJobQueue = HBRedisJobQueue(
            redisService,
            configuration: .init(
                pendingJobInitialization: .remove,
                processingJobsInitialization: .remove,
                failedJobsInitialization: failedJobsInitialization
            )
        )
        var logger = Logger(label: "HummingbirdJobsTests")
        logger.logLevel = .trace
        let jobQueueHandler = HBJobQueueHandler(
            queue: redisJobQueue,
            numWorkers: numWorkers,
            logger: logger
        )

        return try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueueHandler] + additionalServices,
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            let value = try await test(redisJobQueue)
            await serviceGroup.triggerGracefulShutdown()
            return value
        }
    }

    func testBasic() async throws {
        struct TestJob: HBJob {
            static let name = "testBasic"
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

            let value: Int
            func execute(logger: Logger) async throws {
                print(self.value)
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                Self.expectation.fulfill()
            }
        }
        TestJob.register()
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            try await jobQueue.push(TestJob(value: 1))
            try await jobQueue.push(TestJob(value: 2))
            try await jobQueue.push(TestJob(value: 3))
            try await jobQueue.push(TestJob(value: 4))
            try await jobQueue.push(TestJob(value: 5))
            try await jobQueue.push(TestJob(value: 6))
            try await jobQueue.push(TestJob(value: 7))
            try await jobQueue.push(TestJob(value: 8))
            try await jobQueue.push(TestJob(value: 9))
            try await jobQueue.push(TestJob(value: 10))

            await self.wait(for: [TestJob.expectation], timeout: 5)

            let pendingJobs = try await jobQueue.redisConnectionPool.llen(of: jobQueue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)
        }
    }

    func testMultipleWorkers() async throws {
        struct TestJob: HBJob {
            static let name = "testMultipleWorkers"
            static let runningJobCounter = ManagedAtomic(0)
            static let maxRunningJobCounter = ManagedAtomic(0)
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

            let value: Int
            func execute(logger: Logger) async throws {
                let runningJobs = Self.runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
                if runningJobs > Self.maxRunningJobCounter.load(ordering: .relaxed) {
                    Self.maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<100)))
                print(self.value)
                Self.expectation.fulfill()
                Self.runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
            }
        }
        TestJob.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob(value: 1))
            try await jobQueue.push(TestJob(value: 2))
            try await jobQueue.push(TestJob(value: 3))
            try await jobQueue.push(TestJob(value: 4))
            try await jobQueue.push(TestJob(value: 5))
            try await jobQueue.push(TestJob(value: 6))
            try await jobQueue.push(TestJob(value: 7))
            try await jobQueue.push(TestJob(value: 8))
            try await jobQueue.push(TestJob(value: 9))
            try await jobQueue.push(TestJob(value: 10))

            await self.wait(for: [TestJob.expectation], timeout: 5)

            XCTAssertGreaterThan(TestJob.maxRunningJobCounter.load(ordering: .relaxed), 1)
            XCTAssertLessThanOrEqual(TestJob.maxRunningJobCounter.load(ordering: .relaxed), 4)

            let pendingJobs = try await jobQueue.redisConnectionPool.llen(of: jobQueue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)
        }
    }

    func testErrorRetryCount() async throws {
        struct FailedError: Error {}

        struct TestJob: HBJob {
            static let name = "testErrorRetryCount"
            static let maxRetryCount = 3
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 4)
            func execute(logger: Logger) async throws {
                Self.expectation.fulfill()
                throw FailedError()
            }
        }
        TestJob.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob())

            await self.wait(for: [TestJob.expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.redisConnectionPool.llen(of: jobQueue.configuration.failedQueueKey).get()
            XCTAssertEqual(failedJobs, 1)

            let pendingJobs = try await jobQueue.redisConnectionPool.llen(of: jobQueue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)
        }
    }

    /// Test job is cancelled on shutdown
    func testShutdownJob() async throws {
        struct TestJob: HBJob {
            static let name = "testShutdownJob"
            func execute(logger: Logger) async throws {
                try await Task.sleep(for: .milliseconds(100))
            }
        }
        TestJob.register()

        let redisConnectionPool = try RedisConnectionPool(
            .init(hostname: Self.redisHostname, port: 6379),
            logger: Logger(label: "Redis")
        )
        let redis = RedisConnectionPoolService(pool: redisConnectionPool)
        var logger = Logger(label: "HummingbirdJobsTests")
        logger.logLevel = .trace
        let jobQueue = try await self.testJobQueue(redis: redis, numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob())
            return jobQueue
        }

        let pendingJobs = try await jobQueue.redisConnectionPool.llen(of: jobQueue.configuration.queueKey).get()
        XCTAssertEqual(pendingJobs, 0)
        let failedJobs = try await jobQueue.redisConnectionPool.llen(of: jobQueue.configuration.failedQueueKey).get()
        XCTAssertEqual(failedJobs, 1)

        try await redis.close()
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        struct TestJob1: HBJob {
            static let name = "testFailToDecode"
            func execute(logger: Logger) async throws {}
        }
        struct TestJob2: HBJob {
            static let name = "testFailToDecode2"
            static var value: String?
            static let expectation = XCTestExpectation(description: "TestJob2.execute was called")
            let value: String
            func execute(logger: Logger) async throws {
                Self.value = self.value
                Self.expectation.fulfill()
            }
        }
        TestJob2.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob1())
            try await jobQueue.push(TestJob2(value: "test"))
            // stall to give job chance to start running
            await self.wait(for: [TestJob2.expectation], timeout: 5)
            let pendingJobs = try await jobQueue.redisConnectionPool.llen(of: jobQueue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)
        }

        XCTAssertEqual(TestJob2.value, "test")
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    func testRerunAtStartup() async throws {
        struct RetryError: Error {}
        struct TestJob: HBJob {
            static let name = "testRerunAtStartup"
            static let maxRetryCount: Int = 0
            static var firstTime = ManagedAtomic(true)
            static var finished = ManagedAtomic(false)
            static let failedExpectation = XCTestExpectation(description: "TestJob failed")
            static let succeededExpectation = XCTestExpectation(description: "TestJob2 succeeded")
            func execute(logger: Logger) async throws {
                if Self.firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                    Self.failedExpectation.fulfill()
                    throw RetryError()
                }
                Self.succeededExpectation.fulfill()
                Self.finished.store(true, ordering: .relaxed)
            }
        }
        TestJob.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob())

            await self.wait(for: [TestJob.failedExpectation], timeout: 10)

            // stall to give job chance to start running
            try await Task.sleep(for: .milliseconds(50))

            XCTAssertFalse(TestJob.firstTime.load(ordering: .relaxed))
            XCTAssertFalse(TestJob.finished.load(ordering: .relaxed))
        }

        try await self.testJobQueue(numWorkers: 4, failedJobsInitialization: .rerun) { _ in
            await self.wait(for: [TestJob.succeededExpectation], timeout: 10)
            XCTAssertTrue(TestJob.finished.load(ordering: .relaxed))
        }
    }
}
