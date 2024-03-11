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

// Based of the Vapor redis configuration that can be found
// here https://github.com/vapor/redis/blob/master/Sources/Redis/RedisConfiguration.swift
import struct Foundation.URL
import Hummingbird
import Logging
import NIOCore
import RediStack

public struct RedisConfiguration {
    public typealias ValidationError = RedisConnection.Configuration.ValidationError

    public var serverAddresses: [SocketAddress]
    public var password: String?
    public var database: Int?
    public var pool: PoolOptions

    public struct PoolOptions {
        public var maximumConnectionCount: RedisConnectionPoolSize
        public var minimumConnectionCount: Int
        public var connectionBackoffFactor: Float32
        public var initialConnectionBackoffDelay: TimeAmount
        public var connectionRetryTimeout: TimeAmount?

        public init(
            maximumConnectionCount: RedisConnectionPoolSize = .maximumActiveConnections(2),
            minimumConnectionCount: Int = 0,
            connectionBackoffFactor: Float32 = 2,
            initialConnectionBackoffDelay: TimeAmount = .milliseconds(100),
            connectionRetryTimeout: TimeAmount? = nil
        ) {
            self.maximumConnectionCount = maximumConnectionCount
            self.minimumConnectionCount = minimumConnectionCount
            self.connectionBackoffFactor = connectionBackoffFactor
            self.initialConnectionBackoffDelay = initialConnectionBackoffDelay
            self.connectionRetryTimeout = connectionRetryTimeout
        }
    }

    public init(url string: String, pool: PoolOptions = .init()) throws {
        guard let url = URL(string: string) else { throw ValidationError.invalidURLString }
        try self.init(url: url, pool: pool)
    }

    public init(url: URL, pool: PoolOptions = .init()) throws {
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
            pool: pool
        )
    }

    public init(
        hostname: String,
        port: Int = RedisConnection.Configuration.defaultPort,
        password: String? = nil,
        database: Int? = nil,
        pool: PoolOptions = .init()
    ) throws {
        if database != nil, database! < 0 { throw ValidationError.outOfBoundsDatabaseID }

        try self.init(
            serverAddresses: [.makeAddressResolvingHost(hostname, port: port)],
            password: password,
            database: database,
            pool: pool
        )
    }

    public init(
        serverAddresses: [SocketAddress],
        password: String? = nil,
        database: Int? = nil,
        pool: PoolOptions = .init()
    ) throws {
        self.serverAddresses = serverAddresses
        self.password = password
        self.database = database
        self.pool = pool
    }
}

extension RedisConnectionPool.Configuration {
    init(
        _ config: RedisConfiguration,
        logger: Logger
    ) {
        self.init(
            initialServerConnectionAddresses: config.serverAddresses,
            maximumConnectionCount: config.pool.maximumConnectionCount,
            connectionFactoryConfiguration: .init(
                connectionInitialDatabase: config.database,
                connectionPassword: config.password,
                connectionDefaultLogger: logger,
                tcpClient: nil
            ),
            minimumConnectionCount: config.pool.minimumConnectionCount,
            connectionBackoffFactor: config.pool.connectionBackoffFactor,
            initialConnectionBackoffDelay: config.pool.initialConnectionBackoffDelay,
            connectionRetryTimeout: config.pool.connectionRetryTimeout,
            poolDefaultLogger: logger
        )
    }
}
