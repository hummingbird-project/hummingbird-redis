//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2022 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Hummingbird
import Lifecycle
import LifecycleNIOCompat
import RediStack

/// Store Redis connection pool array for an EventLoopGroup.
///
/// Provides a connection pool for each EventLoop in the EventLoopGroup
public struct HBRedisConnectionPoolGroup {
    /// Initialise HBRedisConnectionPoolGroup
    /// - Parameters:
    ///   - configuration: redis configuration
    ///   - eventLoopGroup: EventLoopGroup to create connections for
    ///   - logger: Logger
    public init(configuration: HBRedisConfiguration, eventLoopGroup: EventLoopGroup, logger: Logger) {
        var pools: [EventLoop.RedisKey: RedisConnectionPool] = [:]
        for eventLoop in eventLoopGroup.makeIterator() {
            pools[eventLoop.redisKey] = RedisConnectionPool(
                configuration: .init(configuration, defaultLogger: logger),
                boundEventLoop: eventLoop
            )
        }
        self.pools = pools
        self.eventLoopGroup = eventLoopGroup
    }

    /// Get connection pool for EventLoop
    /// - Parameter eventLoop: eventLoop
    /// - Returns: connection pool
    public func pool(for eventLoop: EventLoop) -> RedisConnectionPool {
        guard let pool = self.pools[eventLoop.redisKey] else {
            preconditionFailure("EventLoop must be from Application EventLoopGroup")
        }
        return pool
    }

    /// Close all connection pools
    /// - Returns: EventLoopFuture for when it is done
    func closePools() -> EventLoopFuture<Void> {
        let poolCloseFutures: [EventLoopFuture<Void>] = self.pools.values.map { pool in
            let promise = pool.eventLoop.makePromise(of: Void.self)
            pool.close(promise: promise)
            return promise.futureResult
        }
        return EventLoopFuture.andAllComplete(poolCloseFutures, on: self.eventLoopGroup.next())
    }

    let pools: [EventLoop.RedisKey: RedisConnectionPool]
    let eventLoopGroup: EventLoopGroup
}

extension EventLoop {
    typealias RedisKey = ObjectIdentifier
    var redisKey: RedisKey {
        ObjectIdentifier(self)
    }
}

/// Extend `HBRedisConnectionPoolGroup`` to provide ``RedisClient`` functionality
extension HBRedisConnectionPoolGroup: RedisClient {
    public func unsubscribe(from channels: [RediStack.RedisChannelName], eventLoop: NIOCore.EventLoop? = nil, logger: Logging.Logger? = nil) -> NIOCore.EventLoopFuture<Void> {
        self.pool(for: eventLoop ?? self.eventLoop).unsubscribe(from: channels, eventLoop: nil, logger: logger)
    }

    public func send<CommandResult>(_ command: RediStack.RedisCommand<CommandResult>, eventLoop: NIOCore.EventLoop? = nil, logger: Logging.Logger? = nil) -> NIOCore.EventLoopFuture<CommandResult> {
        self.pool(for: eventLoop ?? self.eventLoop).send(command, eventLoop: nil, logger: logger)
    }

    public func punsubscribe(from patterns: [String], eventLoop: NIOCore.EventLoop? = nil, logger: Logging.Logger? = nil) -> NIOCore.EventLoopFuture<Void> {
        self.pool(for: eventLoop ?? self.eventLoop).punsubscribe(from: patterns, eventLoop: nil, logger: logger)
    }

    public var eventLoop: EventLoop {
        self.eventLoopGroup.next()
    }

    public func logging(to logger: Logger) -> RedisClient {
        self.pool(for: self.eventLoop)
            .logging(to: logger)
    }
}
