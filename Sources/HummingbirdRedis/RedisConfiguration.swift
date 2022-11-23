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

// This is almost a wholesale copy of the Vapor redis configuration that can be found
// here https://github.com/vapor/redis/blob/master/Sources/Redis/RedisConfiguration.swift
@_exported import struct Foundation.URL
@_exported import struct Logging.Logger
import enum NIO.SocketAddress
@_exported import struct NIO.TimeAmount
import RediStack

public struct HBRedisConfiguration {
    public typealias ValidationError = RedisConnection.Configuration.ValidationError

    public var serverAddresses: [SocketAddress]
    public var password: String?
    public var database: Int?
    public var connectionCountBehavior: RedisConnectionPool.ConnectionCountBehavior
    public var connectionRetryStrategy: RedisConnectionPool.PoolConnectionRetryStrategy

    public init(
        url string: String,
        connectionCountBehavior: RedisConnectionPool.ConnectionCountBehavior = .elastic(maximumConnectionCount: 2),
        connectionRetryStrategy: RedisConnectionPool.PoolConnectionRetryStrategy = .exponentialBackoff()
    ) throws {
        guard let url = URL(string: string) else { throw ValidationError.invalidURLString }
        try self.init(
            url: url,
            connectionCountBehavior: connectionCountBehavior,
            connectionRetryStrategy: connectionRetryStrategy
        )
    }

    public init(
        url: URL,
        connectionCountBehavior: RedisConnectionPool.ConnectionCountBehavior = .elastic(maximumConnectionCount: 2),
        connectionRetryStrategy: RedisConnectionPool.PoolConnectionRetryStrategy = .exponentialBackoff()
    ) throws {
        guard
            let scheme = url.scheme,
            !scheme.isEmpty
        else { throw ValidationError.missingURLScheme }
        guard scheme == "redis" else { throw ValidationError.invalidURLScheme }
        guard let host = url.host, !host.isEmpty else { throw ValidationError.missingURLHost }

        try self.init(
            hostname: host,
            port: url.port ?? RedisConnection.Configuration.defaultPort,
            password: url.password,
            database: Int(url.lastPathComponent),
            connectionCountBehavior: connectionCountBehavior,
            connectionRetryStrategy: connectionRetryStrategy
        )
    }

    public init(
        hostname: String,
        port: Int = RedisConnection.Configuration.defaultPort,
        password: String? = nil,
        database: Int? = nil,
        connectionCountBehavior: RedisConnectionPool.ConnectionCountBehavior = .elastic(maximumConnectionCount: 2),
        connectionRetryStrategy: RedisConnectionPool.PoolConnectionRetryStrategy = .exponentialBackoff()
    ) throws {
        if database != nil, database! < 0 { throw ValidationError.outOfBoundsDatabaseID }

        try self.init(
            serverAddresses: [.makeAddressResolvingHost(hostname, port: port)],
            password: password,
            database: database,
            connectionCountBehavior: connectionCountBehavior,
            connectionRetryStrategy: connectionRetryStrategy
        )
    }

    public init(
        serverAddresses: [SocketAddress],
        password: String? = nil,
        database: Int? = nil,
        connectionCountBehavior: RedisConnectionPool.ConnectionCountBehavior = .elastic(maximumConnectionCount: 2),
        connectionRetryStrategy: RedisConnectionPool.PoolConnectionRetryStrategy = .exponentialBackoff()
    ) throws {
        self.serverAddresses = serverAddresses
        self.password = password
        self.database = database
        self.connectionCountBehavior = connectionCountBehavior
        self.connectionRetryStrategy = connectionRetryStrategy
    }
}

extension RedisConnectionPool.Configuration {
    internal init(_ config: HBRedisConfiguration, defaultLogger: Logger) {
        self.init(
            initialServerConnectionAddresses: config.serverAddresses,
            connectionCountBehavior: config.connectionCountBehavior,
            connectionConfiguration: .init(
                initialDatabase: config.database,
                password: config.password,
                defaultLogger: defaultLogger,
                tcpClient: nil
            ),
            retryStrategy: config.connectionRetryStrategy,
            poolDefaultLogger: defaultLogger
        )
    }
}
