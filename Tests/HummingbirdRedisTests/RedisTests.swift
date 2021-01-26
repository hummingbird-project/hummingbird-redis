import Hummingbird
@testable import HummingbirdRedis
import XCTest

final class HummingbirdRedisTests: XCTestCase {
    func testApplicationRedis() throws {
        let app = HBApplication()
        try app.addRedis(configuration: .init(hostname: "localhost", port: 6379))

        let info = try app.redis.send(command: "INFO").wait()
        XCTAssertEqual(info.string?.contains("redis_version"), true)
    }

    func testRouteHandlerRedis() throws {
/*        let app = HBApplication(configuration: .init(address: .hostname(port: Int.random(in: 4000..<9000))))
        try app.redis.initialize(configuration: .init(hostname: "localhost", port: 6379))
        app.router.get("redis") { req in
            req.redis.send(command: "INFO").map {
                $0.description
            }
        }
        app.start()
        defer { app.stop(); app.wait() }

        let client = HTTPClient(eventLoopGroupProvider: .shared(app.eventLoopGroup))
        defer { XCTAssertNoThrow(try client.syncShutdown()) }

        let future = client.get(url: "http://localhost:\(app.configuration.address.port!)/redis").flatMapThrowing { response in
            var body = try XCTUnwrap(response.body)
            XCTAssertEqual(body.readString(length: body.readableBytes)?.contains("redis_version"), true)
        }
        XCTAssertNoThrow(try future.wait())*/
    }
}
