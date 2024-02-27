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
import Logging
import NIOConcurrencyHelpers
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
        redis: HBRedisConnectionPoolService? = nil,
        numWorkers: Int,
        failedJobsInitialization: HBRedisQueue.JobInitialization = .remove,
        test: (HBJobQueue<HBRedisQueue>) async throws -> T
    ) async throws -> T {
        let redisService: HBRedisConnectionPoolService
        var additionalServices: [any Service] = []
        if let redis {
            redisService = redis
        } else {
            redisService = try HBRedisConnectionPoolService(
                .init(hostname: Self.redisHostname, port: 6379),
                logger: Logger(label: "Redis")
            )
            additionalServices = [redisService]
        }
        var logger = Logger(label: "HummingbirdJobsTests")
        logger.logLevel = .trace
        let jobQueue = HBJobQueue(
            .redis(
                redisService,
                configuration: .init(
                    pendingJobInitialization: .remove,
                    processingJobsInitialization: .remove,
                    failedJobsInitialization: failedJobsInitialization
                )
            ),
            numWorkers: numWorkers,
            logger: logger
        )

        return try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: additionalServices + [jobQueue],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            let value = try await test(jobQueue)
            await serviceGroup.triggerGracefulShutdown()
            return value
        }
    }

    func testBasic() async throws {
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }
            try await jobQueue.push(id: jobIdentifer, parameters: 1)
            try await jobQueue.push(id: jobIdentifer, parameters: 2)
            try await jobQueue.push(id: jobIdentifer, parameters: 3)
            try await jobQueue.push(id: jobIdentifer, parameters: 4)
            try await jobQueue.push(id: jobIdentifer, parameters: 5)
            try await jobQueue.push(id: jobIdentifer, parameters: 6)
            try await jobQueue.push(id: jobIdentifer, parameters: 7)
            try await jobQueue.push(id: jobIdentifer, parameters: 8)
            try await jobQueue.push(id: jobIdentifer, parameters: 9)
            try await jobQueue.push(id: jobIdentifer, parameters: 10)

            await self.wait(for: [expectation], timeout: 5)
        }
    }

    func testMultipleWorkers() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let runningJobCounter = ManagedAtomic(0)
        let maxRunningJobCounter = ManagedAtomic(0)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { parameters, context in
                let runningJobs = runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
                if runningJobs > maxRunningJobCounter.load(ordering: .relaxed) {
                    maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                context.logger.info("Parameters=\(parameters)")
                expectation.fulfill()
                runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
            }

            try await jobQueue.push(id: jobIdentifer, parameters: 1)
            try await jobQueue.push(id: jobIdentifer, parameters: 2)
            try await jobQueue.push(id: jobIdentifer, parameters: 3)
            try await jobQueue.push(id: jobIdentifer, parameters: 4)
            try await jobQueue.push(id: jobIdentifer, parameters: 5)
            try await jobQueue.push(id: jobIdentifer, parameters: 6)
            try await jobQueue.push(id: jobIdentifer, parameters: 7)
            try await jobQueue.push(id: jobIdentifer, parameters: 8)
            try await jobQueue.push(id: jobIdentifer, parameters: 9)
            try await jobQueue.push(id: jobIdentifer, parameters: 10)

            await self.wait(for: [expectation], timeout: 5)

            XCTAssertGreaterThan(maxRunningJobCounter.load(ordering: .relaxed), 1)
            XCTAssertLessThanOrEqual(maxRunningJobCounter.load(ordering: .relaxed), 4)
        }
    }

    func testErrorRetryCount() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 4)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(jobIdentifer, maxRetryCount: 3) { _, _ in
                expectation.fulfill()
                throw FailedError()
            }
            try await jobQueue.push(id: jobIdentifer, parameters: 0)

            await self.wait(for: [expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.redisConnectionPool.llen(of: jobQueue.queue.configuration.failedQueueKey).get()
            XCTAssertEqual(failedJobs, 1)

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.llen(of: jobQueue.queue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)
        }
    }

    func testJobSerialization() async throws {
        struct TestJobParameters: Codable {
            let id: Int
            let message: String
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called")
        let jobIdentifer = HBJobIdentifier<TestJobParameters>(#function)
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { parameters, _ in
                XCTAssertEqual(parameters.id, 23)
                XCTAssertEqual(parameters.message, "Hello!")
                expectation.fulfill()
            }
            try await jobQueue.push(id: jobIdentifer, parameters: .init(id: 23, message: "Hello!"))

            await self.wait(for: [expectation], timeout: 5)
        }
    }

    /// Test job is cancelled on shutdown
    func testShutdownJob() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let redis = try HBRedisConnectionPoolService(
            .init(hostname: Self.redisHostname, port: 6379),
            logger: Logger(label: "Redis")
        )
        var logger = Logger(label: "HummingbirdJobsTests")
        logger.logLevel = .trace
        let jobQueue = try await self.testJobQueue(redis: redis, numWorkers: 4) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { _, _ in
                expectation.fulfill()
                try await Task.sleep(for: .milliseconds(1000))
            }
            try await jobQueue.push(id: jobIdentifer, parameters: 0)
            await self.wait(for: [expectation], timeout: 5)
            return jobQueue
        }

        let pendingJobs = try await jobQueue.queue.redisConnectionPool.llen(of: jobQueue.queue.configuration.queueKey).get()
        XCTAssertEqual(pendingJobs, 0)
        let failedJobs = try await jobQueue.queue.redisConnectionPool.llen(of: jobQueue.queue.configuration.failedQueueKey).get()
        let processingJobs = try await jobQueue.queue.redisConnectionPool.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        XCTAssertEqual(failedJobs + processingJobs, 1)

        try await redis.close()
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        let string: NIOLockedValueBox<String> = .init("")
        let jobIdentifer1 = HBJobIdentifier<Int>(#function)
        let jobIdentifer2 = HBJobIdentifier<String>(#function)
        let expectation = XCTestExpectation(description: "job was called", expectedFulfillmentCount: 1)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(jobIdentifer2) { parameters, _ in
                string.withLockedValue { $0 = parameters }
                expectation.fulfill()
            }
            try await jobQueue.push(id: jobIdentifer1, parameters: 2)
            try await jobQueue.push(id: jobIdentifer2, parameters: "test")
            await self.wait(for: [expectation], timeout: 5)
        }
        string.withLockedValue {
            XCTAssertEqual($0, "test")
        }
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    func testRerunAtStartup() async throws {
        struct RetryError: Error {}
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let firstTime = ManagedAtomic(true)
        let finished = ManagedAtomic(false)
        let failedExpectation = XCTestExpectation(description: "TestJob failed", expectedFulfillmentCount: 1)
        let succeededExpectation = XCTestExpectation(description: "TestJob2 succeeded", expectedFulfillmentCount: 1)
        let job = HBJobDefinition(id: jobIdentifer) { _, _ in
            if firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                failedExpectation.fulfill()
                throw RetryError()
            }
            succeededExpectation.fulfill()
            finished.store(true, ordering: .relaxed)
        }
        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(job)

            try await jobQueue.push(id: jobIdentifer, parameters: 0)

            await self.wait(for: [failedExpectation], timeout: 10)

            // stall to give job chance to start running
            try await Task.sleep(for: .milliseconds(50))

            XCTAssertFalse(firstTime.load(ordering: .relaxed))
            XCTAssertFalse(finished.load(ordering: .relaxed))
        }

        try await self.testJobQueue(numWorkers: 4, failedJobsInitialization: .rerun) { jobQueue in
            jobQueue.registerJob(job)
            await self.wait(for: [succeededExpectation], timeout: 10)
            XCTAssertTrue(finished.load(ordering: .relaxed))
        }
    }

    func testMultipleJobQueueHandlers() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 200)
        let logger = {
            var logger = Logger(label: "HummingbirdJobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let job = HBJobDefinition(id: jobIdentifer) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        let redisService = try HBRedisConnectionPoolService(
            .init(hostname: Self.redisHostname, port: 6379),
            logger: Logger(label: "Redis")
        )
        let jobQueue = HBJobQueue(
            HBRedisQueue(redisService),
            numWorkers: 2,
            logger: logger
        )
        jobQueue.registerJob(job)
        let jobQueue2 = HBJobQueue(
            HBRedisQueue(redisService),
            numWorkers: 2,
            logger: logger
        )
        jobQueue2.registerJob(job)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue, jobQueue2],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            do {
                for i in 0..<200 {
                    try await jobQueue.push(id: jobIdentifer, parameters: i)
                }
                await self.wait(for: [expectation], timeout: 5)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                XCTFail("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }
}
