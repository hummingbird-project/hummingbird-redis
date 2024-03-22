# Hummingbird Redis Interface

Redis is an open source, in-memory data structure store, used as a database, cache, and message broker.

This is the Hummingbird interface to [RediStack library](https://gitlab.com/mordil/RediStack.git) a Swift driver for Redis.

## Usage

```swift
import Hummingbird
import HummingbirdRedis

let redis = try RedisConnectionPoolService(
    .init(hostname: redisHostname, port: 6379),
    logger: Logger(label: "Redis")
)

// create router and add a single GET /redis route
let router = Router()
router.get("redis") { request, _ -> String in
    let info = try await redis.send(command: "INFO").get()
    return String(describing: info)
}
// create application using router
var app = Application(
    router: router,
    configuration: .init(address: .hostname("127.0.0.1", port: 8080))
)
app.addServices(redis)
// run hummingbird application
try await app.runService()
```

## Documentation

Reference documentation for HummingbirdRedis can be found [here](https://docs.hummingbird.codes/2.0/documentation/hummingbirdredis)