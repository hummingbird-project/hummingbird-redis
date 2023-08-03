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
public struct RedisConnectionPoolGroup {
    /// Initialise RedisConnectionPoolGroup
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

    /// Shutdown connection pool group
    public func shutdown() -> EventLoopFuture<Void> {
        closePools()
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

/// Extend `RedisConnectionPoolGroup`` to provide `RedisClient`` functionality
extension RedisConnectionPoolGroup: RedisClient {
    public var eventLoop: EventLoop {
        self.eventLoopGroup.next()
    }

    public var pubsubClient: RedisClient {
        self.pools.first!.value
    }

    public func logging(to logger: Logger) -> RedisClient {
        self.pool(for: self.eventLoop)
            .logging(to: logger)
    }

    public func send(command: String, with arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        self.pool(for: self.eventLoop)
            .send(command: command, with: arguments)
    }

    public func subscribe(
        to channels: [RedisChannelName],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .subscribe(to: channels, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func unsubscribe(from channels: [RedisChannelName]) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .unsubscribe(from: channels)
    }

    public func psubscribe(
        to patterns: [String],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .psubscribe(to: patterns, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func punsubscribe(from patterns: [String]) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .punsubscribe(from: patterns)
    }
}
