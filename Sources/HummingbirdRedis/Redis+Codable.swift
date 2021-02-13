import Foundation
import NIO
import RediStack

extension RedisClient {
    /// Decodes the value associated with this keyfrom JSON.
    public func get<D: Decodable>(_ key: RedisKey, asJSON type: D.Type) -> EventLoopFuture<D?> {
        return self.get(key, as: Data.self).flatMapThrowing { data in
            try data.map { try JSONDecoder().decode(D.self, from: $0) }
        }
    }

    /// Sets key to JSON from  encodable item.
    public func set<E: Encodable>(_ key: RedisKey, toJSON entity: E) -> EventLoopFuture<Void> {
        do {
            return try self.set(key, to: JSONEncoder().encode(entity))
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
}
