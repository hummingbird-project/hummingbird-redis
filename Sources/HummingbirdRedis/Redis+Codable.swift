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
    /// Decodes the value associated with this keyfrom JSON.
    public func get<D: Decodable>(_ key: RedisKey, asJSON type: D.Type) -> EventLoopFuture<D?> {
        return self.get(key, as: Data.self).flatMapThrowing { data in
            try data.map { try JSONDecoder().decode(D.self, from: $0) }
        }
    }

    /// Sets the value stored in the key provided, overwriting the previous value.
    ///
    /// Any previous expiration set on the key is discarded if the SET operation was successful.
    ///
    /// - Important: Regardless of the type of value stored at the key, it will be overwritten to a string value.
    ///
    /// [https://redis.io/commands/set](https://redis.io/commands/set)
    /// - Parameters:
    ///     - key: The key to use to uniquely identify this value.
    ///     - value: The value to set the key to.
    /// - Returns: An `EventLoopFuture` that resolves if the operation was successful.
    @inlinable
    public func set<Value: Encodable>(_ key: RedisKey, toJSON value: Value) -> EventLoopFuture<Void> {
        do {
            return try self.set(key, to: JSONEncoder().encode(value))
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    /// Sets the key to the provided value with options to control how it is set.
    ///
    /// [https://redis.io/commands/set](https://redis.io/commands/set)
    /// - Important: Regardless of the type of data stored at the key, it will be overwritten to a "string" data type.
    ///
    ///   ie. If the key is a reference to a Sorted Set, its value will be overwritten to be a "string" data type.
    ///
    /// - Parameters:
    ///     - key: The key to use to uniquely identify this value.
    ///     - value: The value to set the key to.
    ///     - condition: The condition under which the key should be set.
    ///     - expiration: The expiration to use when setting the key. No expiration is set if `nil`.
    /// - Returns: A `NIO.EventLoopFuture` indicating the result of the operation;
    ///     `.ok` if the operation was successful and `.conditionNotMet` if the specified `condition` was not met.
    ///
    ///     If the condition `.none` was used, then the result value will always be `.ok`.
    public func set<Value: Encodable>(
        _ key: RedisKey,
        toJSON value: Value,
        onCondition condition: RedisSetCommandCondition,
        expiration: RedisSetCommandExpiration? = nil
    ) -> EventLoopFuture<RedisSetCommandResult> {
        do {
            return try self.set(key, to: JSONEncoder().encode(value), onCondition: condition, expiration: expiration)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

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
            return try self.setnx(key, to: JSONEncoder().encode(value))
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
            return try self.setex(key, to: JSONEncoder().encode(value), expirationInSeconds: expiration)
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
            return try self.psetex(key, to: JSONEncoder().encode(value), expirationInMilliseconds: expiration)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
}
