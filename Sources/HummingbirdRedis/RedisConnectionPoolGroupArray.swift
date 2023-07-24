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

import NIOCore

public final class RedisConnectionPoolGroupArray {
    public struct Identifier: Hashable {
        let id: String
        init(id: String) {
            self.id = id
        }

        static var `default`: Identifier { .init(id: "default") }
    }

    init(configuration: HBRedisConfiguration, eventLoopGroup: EventLoopGroup, logger: Logger) {
        let connectionPool = RedisConnectionPoolGroup(
            configuration: configuration,
            eventLoopGroup: eventLoopGroup,
            logger: logger
        )
        self.default = connectionPool
        self.redisConnectionPools = [.default: connectionPool]
    }

    public func addConnectionPool(id: Identifier, configuration: HBRedisConfiguration, logger: Logger) {
        let connectionPool = RedisConnectionPoolGroup(
            configuration: configuration,
            eventLoopGroup: self.default.eventLoopGroup,
            logger: logger
        )
        self.redisConnectionPools[id] = connectionPool
    }

    public subscript(_ id: Identifier) -> RedisConnectionPoolGroup? {
        return self.redisConnectionPools[id]
    }

    func closePools() -> EventLoopFuture<Void> {
        let closeFutures = self.redisConnectionPools.map { $0.value.closePools() }
        return EventLoopFuture.andAllComplete(closeFutures, on: self.default.eventLoopGroup.any())
    }

    public let `default`: RedisConnectionPoolGroup
    var redisConnectionPools: [Identifier: RedisConnectionPoolGroup]
}
