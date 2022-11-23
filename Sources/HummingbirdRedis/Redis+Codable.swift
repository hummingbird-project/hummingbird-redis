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
import NIO
import RediStack

extension RedisClient {
    /// Sets the key to the provided value if the key does not exist.
    ///
    /// [https://redis.io/commands/setnx](https://redis.io/commands/setnx)
    /// - Important: Regardless of the type of data stored at the key, it will be overwritten to a "string" data type.
    ///
    /// ie. If the key is a reference to a Sorted Set, its value will be overwritten to be a "string" data type.
    /// - Parameters:
    ///     - key: The key to use to uniquely identify this value.
    ///     - value: The value to set the key to.
    /// - Returns: `true` if the operation successfully completed.
    @inlinable
    public func setnx<Value: Encodable>(_ key: RedisKey, toJSON value: Value) -> EventLoopFuture<Bool> {
        do {
            return try self.send(
                .setnx(key, to: JSONEncoder().encode(value)),
                eventLoop: nil,
                logger: nil
            )
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    /// Sets a key to the provided value and an expiration timeout in seconds.
    ///
    /// See [https://redis.io/commands/setex](https://redis.io/commands/setex)
    /// - Important: Regardless of the type of data stored at the key, it will be overwritten to a "string" data type.
    ///
    /// ie. If the key is a reference to a Sorted Set, its value will be overwritten to be a "string" data type.
    /// - Important: The actual expiration used will be the specified value or `1`, whichever is larger.
    /// - Parameters:
    ///     - key: The key to use to uniquely identify this value.
    ///     - value: The value to set the key to.
    ///     - expiration: The number of seconds after which to expire the key.
    /// - Returns: A `NIO.EventLoopFuture` that resolves if the operation was successful.
    @inlinable
    public func setex<Value: Encodable>(
        _ key: RedisKey,
        toJSON value: Value,
        expirationInSeconds expiration: Int
    ) -> EventLoopFuture<Void> {
        do {
            return try self.send(
                .setex(key, to: JSONEncoder().encode(value), expirationInSeconds: expiration),
                eventLoop: nil,
                logger: nil
            )
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    /// Sets a key to the provided value and an expiration timeout in milliseconds.
    ///
    /// See [https://redis.io/commands/psetex](https://redis.io/commands/psetex)
    /// - Important: Regardless of the type of data stored at the key, it will be overwritten to a "string" data type.
    ///
    /// ie. If the key is a reference to a Sorted Set, its value will be overwritten to be a "string" data type.
    /// - Important: The actual expiration used will be the specified value or `1`, whichever is larger.
    /// - Parameters:
    ///     - key: The key to use to uniquely identify this value.
    ///     - value: The value to set the key to.
    ///     - expiration: The number of milliseconds after which to expire the key.
    /// - Returns: A `NIO.EventLoopFuture` that resolves if the operation was successful.
    @inlinable
    public func psetex<Value: Encodable>(
        _ key: RedisKey,
        toJSON value: Value,
        expirationInMilliseconds expiration: Int
    ) -> EventLoopFuture<Void> {
        do {
            return try self.send(
                .psetex(key, to: JSONEncoder().encode(value), expirationInMilliseconds: expiration),
                eventLoop: nil,
                logger: nil
            )
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
}
