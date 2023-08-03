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
extension HBApplication {
    public var redis: RedisConnectionPoolGroup {
        self.redisConnectionPools.default
    }

    public var redisConnectionPools: RedisConnectionPoolGroupArray {
        self.extensions.get(\.redisConnectionPools, error: "To use Redis you need to set it up first. Please call HBApplication.addRedis()")
    }

    public func redis(id: RedisConnectionPoolGroupIdentifier) -> RedisConnectionPoolGroup? {
        self.redisConnectionPools[id]
    }

    /// Add Redis to HBApplication
    /// - Parameter configuration: Redis configuration
    public func addRedis(configuration: HBRedisConfiguration) {
        guard !self.extensions.exists(\.redisConnectionPools) else {
            preconditionFailure("Redis has already been added to the application")
        }
        self.extensions.set(\.redisConnectionPools, value: .init(
            configuration: configuration,
            eventLoopGroup: self.eventLoopGroup,
            logger: self.logger
        )) { redisConnectionPools in
            try redisConnectionPools.closePools().wait()
        }
    }

    /// Add Redis to HBApplication
    /// - Parameter configuration: Redis configuration
    public func addRedis(id: RedisConnectionPoolGroupIdentifier, configuration: HBRedisConfiguration) {
        if !self.extensions.exists(\.redisConnectionPools) {
            self.extensions.set(\.redisConnectionPools, value: .init(
                id: id,
                configuration: configuration,
                eventLoopGroup: self.eventLoopGroup,
                logger: self.logger
            )) { redisConnectionPools in
                try redisConnectionPools.shutdown().wait()
            }
        } else {
            guard self.redisConnectionPools[id] == nil else {
                preconditionFailure("Redis with id: '\(id)' has already been added to the application")
            }
            self.redisConnectionPools.addConnectionPool(id: id, configuration: configuration, logger: self.logger)
        }
    }
}
