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

import NIOCore
import RediStack

extension HBRedisJobQueue {
    /// Redis Job queue configuration
    public struct Configuration {
        let queueKey: RedisKey
        let processingQueueKey: RedisKey
        let rerunProcessing: Bool
        let pollTime: TimeAmount

        public init(
            queueKey: String = "_hbJobQueue",
            pollTime: TimeAmount = .milliseconds(100),
            rerunProcessing: Bool = true
        ) {
            self.queueKey = RedisKey(queueKey)
            self.processingQueueKey = RedisKey("\(queueKey)Processing")
            self.pollTime = pollTime
            self.rerunProcessing = rerunProcessing
        }
    }
}
