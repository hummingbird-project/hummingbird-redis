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
    /// what to do with failed/processing jobs from last time queue was handled
    public enum JobInitialization: Sendable {
        case doNothing
        case rerun
        case remove
    }

    /// Redis Job queue configuration
    public struct Configuration: Sendable {
        let queueKey: RedisKey
        let processingQueueKey: RedisKey
        let failedQueueKey: RedisKey
        let pendingJobInitialization: JobInitialization
        let processingJobsInitialization: JobInitialization
        let failedJobsInitialization: JobInitialization
        let pollTime: Duration

        public init(
            queueKey: String = "_hbJobQueue",
            pollTime: Duration = .milliseconds(100),
            pendingJobInitialization: JobInitialization = .doNothing,
            processingJobsInitialization: JobInitialization = .rerun,
            failedJobsInitialization: JobInitialization = .doNothing
        ) {
            self.queueKey = RedisKey(queueKey)
            self.processingQueueKey = RedisKey("\(queueKey)Processing")
            self.failedQueueKey = RedisKey("\(queueKey)Failed")
            self.pollTime = pollTime
            self.pendingJobInitialization = pendingJobInitialization
            self.processingJobsInitialization = processingJobsInitialization
            self.failedJobsInitialization = failedJobsInitialization
        }
    }
}

extension RedisKey: @unchecked Sendable {}
