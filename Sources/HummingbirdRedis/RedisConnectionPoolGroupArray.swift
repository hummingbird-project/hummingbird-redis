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

import Logging
import NIOCore

public struct RedisConnectionPoolGroupIdentifier: Hashable, ExpressibleByStringLiteral {
    let id: String

    public init(stringLiteral: String) {
        self.id = stringLiteral
    }

    public init(id: String) {
        self.id = id
    }

    internal static var `default`: Self { .init(id: "_hb_default_") }
}

public final class RedisConnectionPoolGroupArray {
    init(id: RedisConnectionPoolGroupIdentifier = .default, configuration: HBRedisConfiguration, eventLoopGroup: EventLoopGroup, logger: Logger) {
        let connectionPool = RedisConnectionPoolGroup(
            configuration: configuration,
            eventLoopGroup: eventLoopGroup,
            logger: logger
        )
        self.default = connectionPool
        self.redisConnectionPools = [id: connectionPool]
    }

    /// Shutdown connection pool group
    func shutdown() -> EventLoopFuture<Void> {
        self.closePools()
    }

    public func addConnectionPool(id: RedisConnectionPoolGroupIdentifier, configuration: HBRedisConfiguration, logger: Logger) {
        let connectionPool = RedisConnectionPoolGroup(
            configuration: configuration,
            eventLoopGroup: self.default.eventLoopGroup,
            logger: logger
        )
        self.redisConnectionPools[id] = connectionPool
    }

    public subscript(_ id: RedisConnectionPoolGroupIdentifier) -> RedisConnectionPoolGroup? {
        return self.redisConnectionPools[id]
    }

    func closePools() -> EventLoopFuture<Void> {
        let closeFutures = self.redisConnectionPools.map { $0.value.closePools() }
        return EventLoopFuture.andAllComplete(closeFutures, on: self.default.eventLoopGroup.any())
    }

    public let `default`: RedisConnectionPoolGroup
    var redisConnectionPools: [RedisConnectionPoolGroupIdentifier: RedisConnectionPoolGroup]
}
