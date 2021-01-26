import Hummingbird
import HummingbirdXCT
@testable import HummingbirdRedis
import XCTest

final class HummingbirdRedisTests: XCTestCase {
    func testApplication() throws {
        let app = HBApplication()
        try app.addRedis(configuration: .init(hostname: "localhost", port: 6379))

        let info = try app.redis.send(command: "INFO").wait()
        XCTAssertEqual(info.string?.contains("redis_version"), true)
    }

    func testRouteHandler() throws {
        let app = HBApplication(testing: .live)
        try app.addRedis(configuration: .init(hostname: "localhost", port: 6379))
        app.router.get("redis") { req in
            req.redis.send(command: "INFO").map {
                $0.description
            }
        }
        app.XCTStart()
        defer { app.XCTStop(); }

        app.XCTExecute(uri: "/redis", method: .GET) { response in
            var body = try XCTUnwrap(response.body)
            XCTAssertEqual(body.readString(length: body.readableBytes)?.contains("redis_version"), true)
        }
    }
}
