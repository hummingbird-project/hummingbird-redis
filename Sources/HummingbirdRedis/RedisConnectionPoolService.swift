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

import Foundation
import Hummingbird
import Logging
import NIOCore
import RediStack
import ServiceLifecycle

/// Wrapper for RedisConnectionPool that conforms to ServiceLifecycle Service
public struct RedisConnectionPoolService: Service, @unchecked Sendable {
    public init(pool: RedisConnectionPool) {
        self.pool = pool
    }

    /// Initialize RedisConnectionPoolService
    public init(
        _ config: HBRedisConfiguration,
        eventLoopGroupProvider: EventLoopGroupProvider = .singleton,
        logger: Logger
    ) {
        self.pool = .init(config, eventLoopGroupProvider: eventLoopGroupProvider, logger: logger)
    }

    public let pool: RedisConnectionPool

    @inlinable
    public func run() async throws {
        await GracefulShutdownWaiter().wait()
        try await self.close()
    }

    /// Starts the connection pool.
    ///
    /// This method is safe to call multiple times.
    /// - Parameter logger: An optional logger to use for any log statements generated while starting up the pool.
    ///         If one is not provided, the pool will use its default logger.
    @inlinable
    public func activate(logger: Logger? = nil) {
        self.pool.activate(logger: logger)
    }

    /// Closes all connections in the pool and deactivates the pool from creating new connections.
    ///
    /// This method is safe to call multiple times.
    /// - Parameters:
    ///     - promise: A notification promise to resolve once the close process has completed.
    ///     - logger: An optional logger to use for any log statements generated while closing the pool.
    ///         If one is not provided, the pool will use its default logger.
    @inlinable
    public func close() async throws {
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.pool.close(promise: promise)
        return try await promise.futureResult.get()
    }
}

extension RedisConnectionPoolService {
    /// A unique identifer to represent this connection.
    @inlinable
    public var id: UUID { self.pool.id }
    /// The count of connections that are active and available for use.
    @inlinable
    public var availableConnectionCount: Int { self.pool.availableConnectionCount }
    /// The number of connections that have been handed out and are in active use.
    @inlinable
    public var leasedConnectionCount: Int { self.pool.leasedConnectionCount }
    /// Provides limited exclusive access to a connection to be used in a user-defined specialized closure of operations.
    /// - Warning: Attempting to create PubSub subscriptions with connections leased in the closure will result in a failed `NIO.EventLoopFuture`.
    ///
    /// `RedisConnectionPool` manages PubSub state and requires exclusive control over creating PubSub subscriptions.
    /// - Important: This connection **MUST NOT** be stored outside of the closure. It is only available exclusively within the closure.
    ///
    /// All operations should be done inside the closure as chained `NIO.EventLoopFuture` callbacks.
    ///
    /// For example:
    /// ```swift
    /// let countFuture = pool.leaseConnection {
    ///     let client = $0.logging(to: myLogger)
    ///     return client.authorize(with: userPassword)
    ///         .flatMap { connection.select(database: userDatabase) }
    ///         .flatMap { connection.increment(counterKey) }
    /// }
    /// ```
    /// - Warning: Some commands change the state of the connection that are not tracked client-side,
    /// and will not be automatically reset when the connection is returned to the pool.
    ///
    /// When the connection is reused from the pool, it will retain this state and may affect future commands executed with it.
    ///
    /// For example, if `select(database:)` is used, all future commands made with this connection will be against the selected database.
    ///
    /// To protect against future issues, make sure the final commands executed are to reset the connection to it's previous known state.
    /// - Parameter operation: A closure that receives exclusive access to the provided `RedisConnection` for the lifetime of the closure for specialized Redis command chains.
    /// - Returns: A `NIO.EventLoopFuture` that resolves the value of the `NIO.EventLoopFuture` in the provided closure operation.
    @inlinable
    public func leaseConnection<T>(_ operation: @escaping (RedisConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        self.pool.leaseConnection(operation)
    }

    /// Updates the list of valid connection addresses.
    /// - Warning: This will replace any previously set list of addresses.
    /// - Note: This does not invalidate existing connections: as long as those connections continue to stay up, they will be kept by
    /// this client.
    ///
    /// However, no new connections will be made to any endpoint that is not in `newAddresses`.
    /// - Parameters:
    ///     - newAddresses: The new addresses to connect to in future connections.
    ///     - logger: An optional logger to use for any log statements generated while updating the target addresses.
    ///         If one is not provided, the pool will use its default logger.
    @inlinable
    public func updateConnectionAddresses(_ newAddresses: [SocketAddress], logger: Logger? = nil) {
        self.pool.updateConnectionAddresses(newAddresses)
    }
}

extension RedisConnectionPoolService: RedisClient {
    @inlinable
    public var eventLoop: NIOCore.EventLoop { self.pool.eventLoop }

    @inlinable
    public func send(command: String, with arguments: [RediStack.RESPValue]) -> NIOCore.EventLoopFuture<RediStack.RESPValue> {
        self.pool.send(command: command, with: arguments)
    }

    @inlinable
    public func logging(to logger: Logging.Logger) -> RediStack.RedisClient {
        self.pool.logging(to: logger)
    }

    @inlinable
    public func unsubscribe(from channels: [RediStack.RedisChannelName]) -> NIOCore.EventLoopFuture<Void> {
        self.pool.unsubscribe(from: channels)
    }

    @inlinable
    public func punsubscribe(from patterns: [String]) -> NIOCore.EventLoopFuture<Void> {
        self.pool.punsubscribe(from: patterns)
    }
}
