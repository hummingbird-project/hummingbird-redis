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
@preconcurrency import RediStack

extension RedisClient {
    /// Decodes the value associated with this keyfrom JSON.
    public func get<D: Decodable>(_ key: RedisKey, asJSON type: D.Type) async throws -> D? {
        guard let data = try await self.get(key, as: Data.self).get() else { return nil }
        return try JSONDecoder().decode(D.self, from: data)
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
    @inlinable
    public func set(_ key: RedisKey, toJSON value: some Encodable) async throws {
        try await self.set(key, to: JSONEncoder().encode(value)).get()
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
    @_disfavoredOverload
    public func set(
        _ key: RedisKey,
        toJSON value: some Encodable,
        onCondition condition: RedisSetCommandCondition = .none,
        expiration: RedisSetCommandExpiration? = nil
    ) async throws -> RedisSetCommandResult {
        try await self.set(key, to: JSONEncoder().encode(value), onCondition: condition, expiration: expiration).get()
    }
}
