# Hummingbird Redis Interface

Redis is an open source, in-memory data structure store, used as a database, cache, and message broker.

This is the Hummingbird interface to [RediStack library](https://gitlab.com/mordil/RediStack.git) a Swift driver for Redis.

## Usage

```swift
let app = HBApplication()
try app.addRedis(configuration: .init(hostname: "localhost", port: 6379))
app.router.get("redis") { request in
    request.redis.send(command: "INFO").map {
        $0.description
    }
}
app.start()
```
