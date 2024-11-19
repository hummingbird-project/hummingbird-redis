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
public struct RedisPersistDriver: PersistDriver {
    let redisConnectionPool: RedisConnectionPoolService

    public init(redisConnectionPoolService: RedisConnectionPoolService) {
        self.redisConnectionPool = redisConnectionPoolService
    }

    /// create new key with value. If key already exist throw `PersistError.duplicate` error
    public func create(key: String, value: some Codable, expires: Duration?) async throws {
        let expiration: RedisSetCommandExpiration? = expires.map { .seconds(Int($0.components.seconds)) }
        let result = try await self.redisConnectionPool.set(.init(key), toJSON: value, onCondition: .keyDoesNotExist, expiration: expiration)
        switch result {
        case .ok:
            return
        case .conditionNotMet:
            throw PersistError.duplicate
        }
    }

    /// set value for key. If value already exists overwrite it
    public func set(key: String, value: some Codable, expires: Duration?) async throws {
        if let expires {
            let expiration = Int(expires.components.seconds)
            _ = try await self.redisConnectionPool.set(
                .init(key),
                toJSON: value,
                onCondition: .none,
                expiration: .seconds(expiration)
            )
        } else {
            _ = try await self.redisConnectionPool.set(
                .init(key),
                toJSON: value,
                onCondition: .none,
                expiration: .keepExisting
            )
        }
    }

    /// get value for key
    public func get<Object: Codable>(key: String, as object: Object.Type) async throws -> Object? {
        do {
            return try await self.redisConnectionPool.get(.init(key), asJSON: object)
        } catch is DecodingError {
            throw PersistError.invalidConversion
        }
    }

    /// remove key
    public func remove(key: String) async throws {
        _ = try await self.redisConnectionPool.delete(.init(key)).get()
    }
}
