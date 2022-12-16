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
    public var eventLoop: NIOCore.EventLoop {
        return request.eventLoop
    }

    public func unsubscribe(from channels: [RediStack.RedisChannelName]) -> NIOCore.EventLoopFuture<Void> {
        self.request.application.redis.pool(for: self.request.eventLoop)
            .unsubscribe(from: channels, eventLoop: request.eventLoop, logger: request.logger)
    }

    public func punsubscribe(from patterns: [String]) -> NIOCore.EventLoopFuture<Void> {
        self.request.application.redis.pool(for: self.request.eventLoop)
            .punsubscribe(from: patterns, eventLoop: request.eventLoop, logger: request.logger)
    }

    public func send<CommandResult>(_ command: RediStack.RedisCommand<CommandResult>) -> NIOCore.EventLoopFuture<CommandResult> {
        self.request.application.redis.pool(for: self.request.eventLoop)
            .send(command, eventLoop: nil, logger: request.logger)
    }

    public func unsubscribe(from channels: [RediStack.RedisChannelName], eventLoop: NIOCore.EventLoop?, logger: Logging.Logger?) -> NIOCore.EventLoopFuture<Void> {
        self.request.application.redis.unsubscribe(from: channels, eventLoop: eventLoop ?? request.eventLoop, logger: logger ?? request.logger)
    }

    public func punsubscribe(from patterns: [String], eventLoop: NIOCore.EventLoop?, logger: Logging.Logger?) -> NIOCore.EventLoopFuture<Void> {
        self.request.application.redis.punsubscribe(from: patterns, eventLoop: eventLoop ?? request.eventLoop, logger: logger ?? request.logger)
    }

    public func send<CommandResult>(_ command: RediStack.RedisCommand<CommandResult>, eventLoop: NIOCore.EventLoop?, logger: Logging.Logger?) -> NIOCore.EventLoopFuture<CommandResult> {
        self.request.application.redis.pool(for: eventLoop ?? self.request.eventLoop)
            .send(command, eventLoop: nil, logger: logger ?? request.logger)
    }
}
