//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2023 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Hummingbird
import Logging
import NIOCore
import RediStack
import ServiceLifecycle

/// Temporary unchecked Sendable struct to hold the currently non-Sendable RedisConnectionPool
public struct RedisConnectionPoolService: Service, @unchecked Sendable {
    public init(pool: RedisConnectionPool) {
        self.pool = pool
    }

    public let pool: RedisConnectionPool

    public func run() async throws {
        await GracefulShutdownWaiter().wait()
        try await self.close()
    }

    public func close() async throws {
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.pool.close(promise: promise)
        return try await promise.futureResult.get()
    }
}

extension RedisConnectionPoolService: RedisClient {
    public var eventLoop: NIOCore.EventLoop { self.pool.eventLoop }

    public func send(command: String, with arguments: [RediStack.RESPValue]) -> NIOCore.EventLoopFuture<RediStack.RESPValue> {
        self.pool.send(command: command, with: arguments)
    }

    public func logging(to logger: Logging.Logger) -> RediStack.RedisClient {
        self.pool.logging(to: logger)
    }

    public func unsubscribe(from channels: [RediStack.RedisChannelName]) -> NIOCore.EventLoopFuture<Void> {
        self.pool.unsubscribe(from: channels)
    }

    public func punsubscribe(from patterns: [String]) -> NIOCore.EventLoopFuture<Void> {
        self.pool.punsubscribe(from: patterns)
    }
}
