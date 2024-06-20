<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/hummingbird-project/hummingbird/assets/9382567/48de534f-8301-44bd-b117-dfb614909efd">
  <img src="https://github.com/hummingbird-project/hummingbird/assets/9382567/e371ead8-7ca1-43e3-8077-61d8b5eab879">
</picture>
</p>  
<p align="center">
<a href="https://swift.org">
  <img src="https://img.shields.io/badge/swift-5.9-brightgreen.svg"/>
</a>
<a href="https://github.com/hummingbird-project/hummingbird-redis/actions?query=workflow%3ACI">
  <img src="https://github.com/hummingbird-project/hummingbird-redis/actions/workflows/ci.yml/badge.svg?branch=main"/>
</a>
<a href="https://discord.gg/7ME3nZ7mP2">
  <img src="https://img.shields.io/badge/chat-discord-brightgreen.svg"/>
</a>
</p>

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
