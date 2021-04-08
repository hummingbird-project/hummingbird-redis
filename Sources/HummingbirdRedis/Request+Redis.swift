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
import RediStack

extension HBRequest {
    public var redis: Redis {
        .init(request: self)
    }

    public struct Redis {
        let request: HBRequest
    }
}

extension HBRequest.Redis: RedisClient {
    public var eventLoop: EventLoop {
        self.request.eventLoop
    }

    public func logging(to logger: Logger) -> RedisClient {
        self.request.application.redis
            .pool(for: self.eventLoop)
            .logging(to: logger)
    }

    public func send(command: String, with arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        self.request.application.redis
            .pool(for: self.eventLoop)
            .logging(to: self.request.logger)
            .send(command: command, with: arguments)
    }

    public func subscribe(
        to channels: [RedisChannelName],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self.request.application.redis
            .pubsubClient
            .logging(to: self.request.logger)
            .subscribe(to: channels, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func unsubscribe(from channels: [RedisChannelName]) -> EventLoopFuture<Void> {
        return self.request.application.redis
            .pubsubClient
            .logging(to: self.request.logger)
            .unsubscribe(from: channels)
    }

    public func psubscribe(
        to patterns: [String],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self.request.application.redis
            .pubsubClient
            .logging(to: self.request.logger)
            .psubscribe(to: patterns, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func punsubscribe(from patterns: [String]) -> EventLoopFuture<Void> {
        return self.request.application.redis
            .pubsubClient
            .logging(to: self.request.logger)
            .punsubscribe(from: patterns)
    }
}
