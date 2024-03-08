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
import RediStack

/// Redis driver for persist system for storing persistent cross request key/value pairs
public struct HBRedisPersistDriver: HBPersistDriver {
    let redisConnectionPool: HBRedisConnectionPoolService

    public init(redisConnectionPoolService: HBRedisConnectionPoolService) {
        self.redisConnectionPool = redisConnectionPoolService
    }

    /// create new key with value. If key already exist throw `HBPersistError.duplicate` error
    public func create(key: String, value: some Codable, expires: Duration?) async throws {
        let expiration: RedisSetCommandExpiration? = expires.map { .seconds(Int($0.components.seconds)) }
        let result = try await self.redisConnectionPool.set(.init(key), toJSON: value, onCondition: .keyDoesNotExist, expiration: expiration).get()
        switch result {
        case .ok:
            return
        case .conditionNotMet:
            throw HBPersistError.duplicate
        }
    }

    /// set value for key. If value already exists overwrite it
    public func set(key: String, value: some Codable, expires: Duration?) async throws {
        if let expires {
            let expiration = Int(expires.components.seconds)
            return try await self.redisConnectionPool.setex(.init(key), toJSON: value, expirationInSeconds: expiration).get()
        } else {
            return try await self.redisConnectionPool.set(.init(key), toJSON: value).get()
        }
    }

    /// get value for key
    public func get<Object: Codable>(key: String, as object: Object.Type) async throws -> Object? {
        try await self.redisConnectionPool.get(.init(key), asJSON: object).get()
    }

    /// remove key
    public func remove(key: String) async throws {
        _ = try await self.redisConnectionPool.delete(.init(key)).get()
    }
}
