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
import struct Foundation.Data
import class Foundation.JSONDecoder
import Hummingbird
import HummingbirdJobs
import HummingbirdRedis
import NIOCore
import RediStack

/// Redis implementation of job queues
public final class HBRedisJobQueue: HBJobQueue {
    public enum RedisQueueError: Error, CustomStringConvertible {
        case unexpectedRedisKeyType
        case jobMissing(JobIdentifier)

        public var description: String {
            switch self {
            case .unexpectedRedisKeyType:
                return "Unexpected redis key type"
            case .jobMissing(let value):
                return "Job associated with \(value) is missing"
            }
        }
    }

    let redisConnectionPool: RedisConnectionPoolService
    let configuration: Configuration
    let isStopped: ManagedAtomic<Bool>
    public var pollTime: TimeAmount { self.configuration.pollTime }

    /// Initialize redis job queue
    /// - Parameters:
    ///   - redisConnectionPoolGroup: Redis connection pool group
    ///   - configuration: configuration
    public init(_ redisConnectionPoolService: RedisConnectionPoolService, configuration: Configuration) {
        self.redisConnectionPool = redisConnectionPoolService
        self.configuration = configuration
        self.isStopped = .init(false)
    }

    /// This is run at initialization time.
    ///
    /// Will push all the jobs in the processing queue back onto to the main queue so they can
    /// be rerun
    public func onInit() async throws {
        try await self.initQueue(queueKey: self.configuration.queueKey, onInit: self.configuration.pendingJobInitialization)
        // there shouldn't be any on the processing list, but if there are we should do something with them
        try await self.initQueue(queueKey: self.configuration.processingQueueKey, onInit: self.configuration.processingJobsInitialization)
        try await self.initQueue(queueKey: self.configuration.failedQueueKey, onInit: self.configuration.failedJobsInitialization)
    }

    /// Push Job onto queue
    /// - Parameters:
    ///   - job: Job descriptor
    /// - Returns: Queued job identifier
    @discardableResult public func push(_ job: any HBJob) async throws -> JobIdentifier {
        let queuedJob = HBQueuedJob(job)
        _ = try await self.set(jobId: queuedJob.id, job: queuedJob.job)
        _ = try await self.redisConnectionPool.lpush(queuedJob.id.redisKey, into: self.configuration.queueKey).get()
        return queuedJob.id
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobId: Job id
    public func finished(jobId: JobIdentifier) async throws {
        _ = try await self.redisConnectionPool.lrem(jobId.description, from: self.configuration.processingQueueKey, count: 0).get()
        try await self.delete(jobId: jobId)
    }

    /// Flag job failed to process
    ///
    /// Removes  job id from processing queue, adds to failed queue
    /// - Parameters:
    ///   - jobId: Job id
    public func failed(jobId: JobIdentifier, error: Error) async throws {
        _ = try await self.redisConnectionPool.lrem(jobId.redisKey, from: self.configuration.processingQueueKey, count: 0).get()
        _ = try await self.redisConnectionPool.lpush(jobId.redisKey, into: self.configuration.failedQueueKey).get()
    }

    public func stop() async {
        self.isStopped.store(true, ordering: .relaxed)
    }

    public func shutdownGracefully() async {}

    /// Pop Job off queue and add to pending queue
    /// - Parameter eventLoop: eventLoop to do work on
    /// - Returns: queued job
    func popFirst() async throws -> HBQueuedJob? {
        let pool = self.redisConnectionPool.pool
        let key = try await pool.rpoplpush(from: self.configuration.queueKey, to: self.configuration.processingQueueKey).get()
        guard !key.isNull else {
            return nil
        }
        guard let key = String(fromRESP: key) else {
            throw RedisQueueError.unexpectedRedisKeyType
        }
        let identifier = JobIdentifier(fromKey: key)
        if let job = try await self.get(jobId: identifier) {
            return .init(id: identifier, job: job)
        } else {
            throw RedisQueueError.jobMissing(identifier)
        }
    }

    /// What to do with queue at initialization
    func initQueue(queueKey: RedisKey, onInit: JobInitialization) async throws {
        switch onInit {
        case .remove:
            try await self.remove(queueKey: queueKey)
        case .rerun:
            try await self.rerun(queueKey: queueKey)
        case .doNothing:
            break
        }
    }

    /// Push all the entries from list back onto the main list.
    func rerun(queueKey: RedisKey) async throws {
        while true {
            let key = try await self.redisConnectionPool.rpoplpush(from: queueKey, to: self.configuration.queueKey).get()
            if key.isNull {
                break
            }
        }
    }

    /// Push all the entries from list back onto the main list.
    func remove(queueKey: RedisKey) async throws {
        while true {
            let key = try await self.redisConnectionPool.rpop(from: queueKey).get()
            if key.isNull {
                break
            }
            guard let key = String(fromRESP: key) else {
                throw RedisQueueError.unexpectedRedisKeyType
            }
            let identifier = JobIdentifier(fromKey: key)
            try await self.delete(jobId: identifier)
        }
    }

    func get(jobId: JobIdentifier) async throws -> HBJobInstance? {
        guard let data = try await self.redisConnectionPool.get(jobId.redisKey, as: Data.self).get() else {
            return nil
        }
        do {
            return try JSONDecoder().decode(HBJobInstance.self, from: data)
        } catch {
            throw JobQueueError.decodeJobFailed
        }
    }

    func set(jobId: JobIdentifier, job: HBJobInstance) async throws {
        return try await self.redisConnectionPool.set(jobId.redisKey, toJSON: job).get()
    }

    func delete(jobId: JobIdentifier) async throws {
        _ = try await self.redisConnectionPool.delete(jobId.redisKey).get()
    }
}

extension HBRedisJobQueue {
    public struct AsyncIterator: AsyncIteratorProtocol {
        let queue: HBRedisJobQueue

        public func next() async throws -> Element? {
            while true {
                if self.queue.isStopped.load(ordering: .relaxed) {
                    return nil
                }
                if let job = try await queue.popFirst() {
                    return job
                }
                try await Task.sleep(for: .milliseconds(100))
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return .init(queue: self)
    }
}

extension JobIdentifier {
    var redisKey: RedisKey { .init(self.description) }

    init(fromKey key: String) {
        self.init(key)
    }
}
