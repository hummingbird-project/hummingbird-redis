# Hummingbird Redis Interface

Hummingbird interface to [RediStack library](https://gitlab.com/mordil/RediStack.git).

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
