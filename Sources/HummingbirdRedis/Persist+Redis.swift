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

/// In memory driver for persist system for storing persistent cross request key/value pairs
struct HBRedisPersistDriver: HBPersistDriver {
    init(application: HBApplication) {
        self.application = application
    }

    func set<Object: Codable>(key: String, value: Object, on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        self.application.redis.pool(for: eventLoop).set(.init(key), toJSON: value)
    }

    func set<Object: Codable>(key: String, value: Object, expires: TimeAmount, on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        self.application.redis.pool(for: eventLoop).setex(.init(key), toJSON: value, expirationInSeconds: Int(expires.nanoseconds / 1_000_000_000))
    }

    func get<Object: Codable>(key: String, as object: Object.Type, on eventLoop: EventLoop) -> EventLoopFuture<Object?> {
        self.application.redis.pool(for: eventLoop).get(.init(key), asJSON: object)
    }

    func remove(key: String, on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        self.application.redis.pool(for: eventLoop).delete(.init(key))
            .map { _ in }
    }

    let application: HBApplication
}

/// Factory class for persist drivers
extension HBPersistDriverFactory {
    /// In memory driver for persist system
    public static var redis: HBPersistDriverFactory {
        .init(create: { app in HBRedisPersistDriver(application: app) })
    }
}
