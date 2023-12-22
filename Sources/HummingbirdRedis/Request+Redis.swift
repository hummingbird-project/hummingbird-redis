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

import Hummingbird
import Logging
import RediStack

extension HBRequest {
    public var redis: Redis {
        .init(eventLoop: self.eventLoop, logger: self.logger, connectionPoolGroup: self.application.redis)
    }

    public func redis(id: RedisConnectionPoolGroupIdentifier) -> Redis {
        guard let redisConnectionPoolGroup = self.application.redis(id: id) else {
            preconditionFailure("Redis Connection Pool Group id: '\(id)' does not exist")
        }
        return .init(eventLoop: self.eventLoop, logger: self.logger, connectionPoolGroup: redisConnectionPoolGroup)
    }

    public struct Redis {
        public let eventLoop: EventLoop
        let logger: Logger
        let connectionPoolGroup: RedisConnectionPoolGroup
    }
}

extension HBRequest.Redis: RedisClient {
    public func logging(to logger: Logger) -> RedisClient {
        self.connectionPoolGroup
            .pool(for: self.eventLoop)
            .logging(to: logger)
    }

    public func send(command: String, with arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        self.connectionPoolGroup
            .pool(for: self.eventLoop)
            .logging(to: self.logger)
            .send(command: command, with: arguments)
    }

    public func subscribe(
        to channels: [RedisChannelName],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self.connectionPoolGroup
            .pubsubClient
            .logging(to: self.logger)
            .subscribe(to: channels, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func unsubscribe(from channels: [RedisChannelName]) -> EventLoopFuture<Void> {
        return self.connectionPoolGroup
            .pubsubClient
            .logging(to: self.logger)
            .unsubscribe(from: channels)
    }

    public func psubscribe(
        to patterns: [String],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self.connectionPoolGroup
            .pubsubClient
            .logging(to: self.logger)
            .psubscribe(to: patterns, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func punsubscribe(from patterns: [String]) -> EventLoopFuture<Void> {
        return self.connectionPoolGroup
            .pubsubClient
            .logging(to: self.logger)
            .punsubscribe(from: patterns)
    }
}
