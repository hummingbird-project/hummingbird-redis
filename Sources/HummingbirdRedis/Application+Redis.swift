import Hummingbird
import Lifecycle
import LifecycleNIOCompat
import RediStack

extension HBApplication {
    /// Redis interface
    public struct Redis {
        init(configuration: RedisConfiguration, application: HBApplication) {
            self.configuration = configuration
            self.application = application
            Self.createPools(configuration: configuration, application: application)
            self.pubsubClient = application.eventLoopStorage.first { _ in true }!.storage.redisPool

            application.lifecycle.registerShutdown(label: "Redis", .eventLoopFuture(self.closePools))
        }

        public func pool(for eventLoop: EventLoop) -> RedisConnectionPool {
            return self.application.eventLoopStorage.get(for: eventLoop).redisPool
        }

        private static func createPools(configuration: RedisConfiguration, application: HBApplication) {
            application.eventLoopStorage.forEach { eventLoop, storage in
                storage.redisPool = RedisConnectionPool(
                    configuration: .init(configuration, defaultLogger: application.logger),
                    boundEventLoop: eventLoop
                )
            }
        }

        private func closePools() -> EventLoopFuture<Void> {
            let poolCloseFutures: [EventLoopFuture<Void>] = self.application.eventLoopStorage.map { ev, storage in
                let promise = ev.makePromise(of: Void.self)
                storage.redisPool.close(promise: promise)
                return promise.futureResult
            }
            return EventLoopFuture.andAllComplete(poolCloseFutures, on: self.application.eventLoopGroup.next())
        }

        var pubsubClient: RedisClient

        private unowned var application: HBApplication
        private var configuration: RedisConfiguration
    }

    public var redis: Redis {
        self.extensions.get(\.redis)
    }

    /// Add Redis to HBApplication
    /// - Parameter configuration: Redis configuration
    public func addRedis(configuration: RedisConfiguration) {
        self.extensions.set(\.redis, value: .init(configuration: configuration, application: self))
    }
}

extension HBApplication.EventLoopStorage {
    var redisPool: RedisConnectionPool {
        get { extensions.get(\.redisPool) }
        set { extensions.set(\.redisPool, value: newValue) }
    }
}

extension HBApplication.Redis: RedisClient {
    public var eventLoop: EventLoop {
        self.application.eventLoopGroup.next()
    }

    public func logging(to logger: Logger) -> RedisClient {
        self.pool(for: self.eventLoop)
            .logging(to: logger)
    }

    public func send(command: String, with arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        self.pool(for: self.eventLoop)
            .logging(to: self.application.logger)
            .send(command: command, with: arguments)
    }

    public func subscribe(
        to channels: [RedisChannelName],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .logging(to: self.application.logger)
            .subscribe(to: channels, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func unsubscribe(from channels: [RedisChannelName]) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .logging(to: self.application.logger)
            .unsubscribe(from: channels)
    }

    public func psubscribe(
        to patterns: [String],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .logging(to: self.application.logger)
            .psubscribe(to: patterns, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    public func punsubscribe(from patterns: [String]) -> EventLoopFuture<Void> {
        return self
            .pubsubClient
            .logging(to: self.application.logger)
            .punsubscribe(from: patterns)
    }
}
