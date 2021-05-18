//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
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

/// In memory driver for persist system for storing persistent cross request key/value pairs
struct HBRedisPersistDriver: HBPersistDriver {
    init(application: HBApplication) {
        precondition(
            application.extensions.exists(\HBApplication.redis),
            "Cannot use Redis driver without having setup Redis. Please call HBApplication.addRedis()"
        )
    }

    /// create new key with value. If key already exist throw `HBPersistError.duplicate` error
    func create<Object: Codable>(key: String, value: Object, expires: TimeAmount?, request: HBRequest) -> EventLoopFuture<Void> {
        let expiration: RedisSetCommandExpiration? = expires.map { .seconds(Int($0.nanoseconds / 1_000_000_000)) }
        return request.redis.set(.init(key), toJSON: value, onCondition: .keyDoesNotExist, expiration: expiration).flatMapThrowing { result in
            switch result {
            case .ok:
                return
            case.conditionNotMet:
                throw HBPersistError.duplicate
            }
        }
    }

    /// set value for key. If value already exists overwrite it
    func set<Object: Codable>(key: String, value: Object, expires: TimeAmount?, request: HBRequest) -> EventLoopFuture<Void> {
        if let expires = expires {
            return request.redis.setex(.init(key), toJSON: value, expirationInSeconds: Int(expires.nanoseconds / 1_000_000_000))
        } else {
            return request.redis.set(.init(key), toJSON: value)
        }
    }

    /// get value for key
    func get<Object: Codable>(key: String, as object: Object.Type, request: HBRequest) -> EventLoopFuture<Object?> {
        request.redis.get(.init(key), asJSON: object)
    }

    /// remove key
    func remove(key: String, request: HBRequest) -> EventLoopFuture<Void> {
        request.redis.delete(.init(key))
            .map { _ in }
    }
}

/// Factory class for persist drivers
extension HBPersistDriverFactory {
    /// In memory driver for persist system
    public static var redis: HBPersistDriverFactory {
        .init(create: { app in HBRedisPersistDriver(application: app) })
    }
}
