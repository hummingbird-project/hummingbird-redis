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
        self.extensions.get(\.redis)
    }

    /// Add Redis to HBApplication
    /// - Parameter configuration: Redis configuration
    public func addRedis(configuration: HBRedisConfiguration) {
        self.extensions.set(\.redis, value: .init(
            configuration: configuration,
            eventLoopGroup: self.eventLoopGroup,
            logger: self.logger
        )) { redis in
            try redis.closePools().wait()
        }
    }
}
