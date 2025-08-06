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
import Valkey

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Valkey/Redis driver for persist system for storing persistent cross request key/value pairs
public struct ValkeyPersistDriver<Client: ValkeyClientProtocol & Sendable>: PersistDriver {
    let valkey: Client

    public init(client: Client) {
        self.valkey = client
    }

    /// create new key with value. If key already exist throw `PersistError.duplicate` error
    public func create(key: String, value: some Codable, expires: Duration?) async throws {
        let expiration: SET<ByteBuffer>.Expiration? = expires.map { .milliseconds(Int($0 / .milliseconds(1))) }
        let jsonBuffer = try ByteBuffer(bytes: JSONEncoder().encode(value))
        if try await self.valkey.set(.init(key), value: jsonBuffer, condition: .nx, expiration: expiration) != nil {
            return
        } else {
            throw PersistError.duplicate
        }
    }

    /// set value for key. If value already exists overwrite it
    public func set(key: String, value: some Codable, expires: Duration?) async throws {
        let jsonBuffer = try ByteBuffer(bytes: JSONEncoder().encode(value))
        if let expires {
            let expiration = SET<ByteBuffer>.Expiration.milliseconds(Int(expires / .milliseconds(1)))
            _ = try await self.valkey.set(
                .init(key),
                value: jsonBuffer,
                condition: .none,
                expiration: expiration
            )
        } else {
            _ = try await self.valkey.set(
                .init(key),
                value: jsonBuffer,
                condition: .none,
                expiration: .keepttl
            )
        }
    }

    /// get value for key
    public func get<Object: Codable>(key: String, as object: Object.Type) async throws -> Object? {
        do {
            if let value = try await self.valkey.get(.init(key)) {
                return try JSONDecoder().decode(Object.self, from: value)
            }
            return nil
        } catch is DecodingError {
            throw PersistError.invalidConversion
        }
    }

    /// remove key
    public func remove(key: String) async throws {
        _ = try await self.valkey.del(keys: [.init(key)])
    }
}
