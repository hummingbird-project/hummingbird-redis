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
import HummingbirdJobs
import HummingbirdRedis
import NIOCore
import RediStack

/// Redis implementation of job queues
public class HBRedisJobQueue: HBJobQueue {
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

    let application: HBApplication
    let configuration: Configuration
    public var pollTime: TimeAmount { self.configuration.pollTime }

    /// Initialize redis job queue
    /// - Parameters:
    ///   - application: Application
    ///   - configuration: Configuration
    public init(_ application: HBApplication, configuration: Configuration) {
        self.application = application
        self.configuration = configuration
    }

    /// This is run at initialization time.
    ///
    /// Will push all the jobs in the processing queue back onto to the main queue so they can
    /// be rerun
    /// - Parameter eventLoop: eventLoop to run process on
    public func onInit(on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        if self.configuration.rerunProcessing {
            return self.rerunProcessing(on: eventLoop)
        } else {
            return eventLoop.makeSucceededVoidFuture()
        }
    }

    /// Push Job onto queue
    /// - Parameters:
    ///   - job: Job descriptor
    ///   - eventLoop: eventLoop to do work on
    /// - Returns: Queued job
    public func push(_ job: HBJob, on eventLoop: EventLoop) -> EventLoopFuture<HBQueuedJob> {
        let pool = self.application.redis.pool(for: eventLoop)
        let queuedJob = HBQueuedJob(job)
        return self.set(jobId: queuedJob.id, job: queuedJob.job, pool: pool)
            .flatMap {
                pool.send(.lpush(queuedJob.id.redisKey, into: self.configuration.queueKey))
            }
            .map { _ in
                return queuedJob
            }
    }

    /// Pop Job off queue
    /// - Parameter eventLoop: eventLoop to do work on
    /// - Returns: queued job
    public func pop(on eventLoop: EventLoop) -> EventLoopFuture<HBQueuedJob?> {
        let pool = self.application.redis.pool(for: eventLoop)
        return pool.send(.rpoplpush(from: self.configuration.queueKey, to: self.configuration.processingQueueKey))
            .flatMap { value -> EventLoopFuture<HBQueuedJob?> in
                guard let value = value else {
                    return eventLoop.makeSucceededFuture(nil)
                }
                guard let key = String(fromRESP: value) else {
                    return eventLoop.makeFailedFuture(RedisQueueError.unexpectedRedisKeyType)
                }
                let identifier = JobIdentifier(fromKey: key)
                return self.get(jobId: identifier, pool: pool)
                    .unwrap(orError: RedisQueueError.jobMissing(identifier))
                    .map { job in
                        return .init(id: identifier, job: job)
                    }
            }
            // temporary fix while Redis throws an error parsing null
            .flatMapErrorThrowing { error in
                switch error {
                case let error as RedisClientError where error == RedisClientError.failedRESPConversion(to: RESPValue?.self):
                    return nil
                default:
                    throw error
                }
            }
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobId: Job id
    ///   - eventLoop: eventLoop to do work on
    public func finished(jobId: JobIdentifier, on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let pool = self.application.redis.pool(for: eventLoop)
        return pool.send(.lrem(jobId.description, from: self.configuration.processingQueueKey, count: 0))
            .flatMap { _ in
                return self.delete(jobId: jobId, pool: pool)
            }
    }

    /// Push all the entries on the processing list back onto the main list.
    ///
    /// This is run at initialization. If a job is in the processing queue at initialization it never was completed the
    /// last time queues were processed so needs to be re run
    public func rerunProcessing(on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        let pool = self.application.redis.pool(for: eventLoop)
        func _moveOneEntry() {
            pool.send(.rpoplpush(from: self.configuration.processingQueueKey, to: self.configuration.queueKey))
                .whenComplete { result in
                    switch result {
                    case .success(let key):
                        if key != nil {
                            _moveOneEntry()
                        } else {
                            promise.succeed(())
                        }
                    case .failure(let error):
                        promise.fail(error)
                    }
                }
        }
        _moveOneEntry()
        return promise.futureResult
    }

    func get(jobId: JobIdentifier, pool: RedisConnectionPool) -> EventLoopFuture<HBJobContainer?> {
        return pool.get(jobId.redisKey, asJSON: HBJobContainer.self)
    }

    func set(jobId: JobIdentifier, job: HBJobContainer, pool: RedisConnectionPool) -> EventLoopFuture<Void> {
        return pool.set(jobId.redisKey, toJSON: job)
    }

    func delete(jobId: JobIdentifier, pool: RedisConnectionPool) -> EventLoopFuture<Void> {
        return pool.delete(jobId.redisKey).map { _ in }
    }
}

extension HBJobQueueFactory {
    /// In memory driver for persist system
    public static func redis(configuration: HBRedisJobQueue.Configuration = .init()) -> HBJobQueueFactory {
        .init(create: { app in HBRedisJobQueue(app, configuration: configuration) })
    }
}

extension JobIdentifier {
    var redisKey: RedisKey { .init(self.description) }

    init(fromKey key: String) {
        self.init(key)
    }
}
