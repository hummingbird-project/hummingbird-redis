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

import Foundation
import Hummingbird
import RediStack

/// Redis driver for persist system for storing persistent cross request key/value pairs
public struct HBRedisPersistDriver: HBPersistDriver {
    let redisConnectionPoolGroup: RedisConnectionPoolGroup

    public init(redisConnectionPoolGroup: RedisConnectionPoolGroup) {
        self.redisConnectionPoolGroup = redisConnectionPoolGroup
    }

    /// create new key with value. If key already exist throw `HBPersistError.duplicate` error
    public func create<Object: Codable>(key: String, value: Object, expires: TimeAmount?, request: HBRequest) -> EventLoopFuture<Void> {
        let expiration: RedisSetCommandExpiration? = expires.map { .seconds(Int($0.nanoseconds / 1_000_000_000)) }
        let redis = self.redisConnectionPoolGroup.pool(for: request.eventLoop)
        return redis.set(.init(key), toJSON: value, onCondition: .keyDoesNotExist, expiration: expiration).flatMapThrowing { result in
            switch result {
            case .ok:
                return
            case .conditionNotMet:
                throw HBPersistError.duplicate
            }
        }
    }

    /// set value for key. If value already exists overwrite it
    public func set<Object: Codable>(key: String, value: Object, expires: TimeAmount?, request: HBRequest) -> EventLoopFuture<Void> {
        let redis = self.redisConnectionPoolGroup.pool(for: request.eventLoop)
        if let expires = expires {
            return redis.setex(.init(key), toJSON: value, expirationInSeconds: Int(expires.nanoseconds / 1_000_000_000))
        } else {
            return redis.set(.init(key), toJSON: value)
        }
    }

    /// get value for key
    public func get<Object: Codable>(key: String, as object: Object.Type, request: HBRequest) -> EventLoopFuture<Object?> {
        let redis = self.redisConnectionPoolGroup.pool(for: request.eventLoop)
        return redis.get(.init(key), asJSON: object)
    }

    /// remove key
    public func remove(key: String, request: HBRequest) -> EventLoopFuture<Void> {
        let redis = self.redisConnectionPoolGroup.pool(for: request.eventLoop)
        return redis.delete(.init(key))
            .map { _ in }
    }
}

/// Factory class for persist drivers
extension HBPersistDriverFactory {
    /// Redis driver for persist system
    public static var redis: HBPersistDriverFactory {
        .init(create: { app in HBRedisPersistDriver(redisConnectionPoolGroup: app.redis) })
    }

    /// Redis driver for persist system
    public static func redis(id: RedisConnectionPoolGroupIdentifier) -> HBPersistDriverFactory {
        .init { app in
            guard let redisConnectionPoolGroup = app.redisConnectionPools[id] else {
                preconditionFailure("Redis Connection Pool Group id: \(id) does not exist")
            }
            return HBRedisPersistDriver(redisConnectionPoolGroup: redisConnectionPoolGroup)
        }
    }
}
