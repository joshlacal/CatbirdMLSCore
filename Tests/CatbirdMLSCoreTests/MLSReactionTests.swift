
import XCTest
import GRDB
import OSLog
@testable import CatbirdMLSCore

final class MLSReactionTests: XCTestCase {
    var dbQueue: DatabaseQueue!
    // MLSStorage requires MLSDatabase (DatabaseWriter) which DatabaseQueue conforms to
    
    override func setUp() async throws {
        try await super.setUp()
        dbQueue = try DatabaseQueue()
        
        try await dbQueue.write { db in
            // Enable Foreign Keys
            try db.execute(sql: "PRAGMA foreign_keys = ON")
            
            // Create MLSMessageModel table (Parent)
            try db.create(table: "MLSMessageModel") { t in
                t.primaryKey("messageID", .text).notNull()
                t.column("currentUserDID", .text).notNull()
                t.column("conversationID", .text).notNull()
                t.column("senderID", .text).notNull()
                t.column("contentType", .text).notNull()
                t.column("timestamp", .datetime).notNull()
                t.column("epoch", .integer).notNull()
                t.column("sequenceNumber", .integer).notNull()
                t.column("isDelivered", .boolean).notNull().defaults(to: false)
                t.column("isRead", .boolean).notNull().defaults(to: false)
                t.column("isSent", .boolean).notNull().defaults(to: false)
                t.column("sendAttempts", .integer).notNull().defaults(to: 0)
                t.column("processingState", .text).notNull()
                t.column("gapBefore", .boolean).notNull().defaults(to: false)
                t.column("payloadExpired", .boolean).notNull().defaults(to: false)
                t.column("processingAttempts", .integer).notNull().defaults(to: 0)
            }
            
            // Create MLSMessageReactionModel (Child with FK)
            try db.create(table: "MLSMessageReactionModel") { t in
                t.primaryKey("reactionID", .text).notNull()
                t.column("messageID", .text).notNull().references("MLSMessageModel", onDelete: .cascade)
                t.column("conversationID", .text).notNull()
                t.column("currentUserDID", .text).notNull()
                t.column("actorDID", .text).notNull()
                t.column("emoji", .text).notNull()
                t.column("action", .text).notNull()
                t.column("timestamp", .datetime).notNull()
            }
            
            // Create MLSOrphanedReactionModel (Backup)
            try db.create(table: "MLSOrphanedReactionModel") { t in
                t.primaryKey("reactionID", .text).notNull()
                t.column("messageID", .text).notNull()
                t.column("conversationID", .text).notNull()
                t.column("currentUserDID", .text).notNull()
                t.column("actorDID", .text).notNull()
                t.column("emoji", .text).notNull()
                t.column("action", .text).notNull()
                t.column("timestamp", .datetime).notNull()
            }
        }
    }
    
    /// Verify that saving a reaction for a missing message saves it as an orphan
    /// instead of throwing a Foreign Key error.
    func testSaveReactionForMissingMessage() async throws {
        let storage = MLSStorage.shared
        // Note: MLSStorage doesn't store the DB, but methods take it as arg.
        
        let reaction = MLSReactionModel(
            messageID: "missing-message-id",
            conversationID: "convo-1",
            currentUserDID: "user-1",
            actorDID: "actor-1",
            emoji: "👍",
            action: "add",
            timestamp: Date()
        )
        
        // This should NOT throw, but instead save as orphan
        try await storage.saveReaction(reaction, database: dbQueue)
        
        // Verify it was saved as orphan
        let orphans = try await dbQueue.read { db in
            try MLSOrphanedReactionModel.fetchAll(db)
        }
        
        XCTAssertEqual(orphans.count, 1)
        XCTAssertEqual(orphans.first?.messageID, "missing-message-id")
        XCTAssertEqual(orphans.first?.emoji, "👍")
        
        // Verify it is NOT in the regular table
        let reacts = try await dbQueue.read { db in
            try MLSReactionModel.fetchAll(db)
        }
        XCTAssertEqual(reacts.count, 0)
    }
    
    /// Verify normal save works when parent exists
    func testSaveReactionForExistingMessage() async throws {
        let storage = MLSStorage.shared
        
        // Create parent message
        try await dbQueue.write { db in
             // We need to construct a valid MLSMessageModel-like insert or use raw SQL
             // Since we don't have easy init access to Model maybe, let's use raw SQL for speed
             try db.execute(sql: """
                INSERT INTO MLSMessageModel (
                    messageID, currentUserDID, conversationID, senderID, contentType,
                    timestamp, epoch, sequenceNumber, processingState
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             """, arguments: [
                "msg-1", "user-1", "convo-1", "sender-1", "text",
                Date(), 0, 1, "processed"
             ])
        }
        
        let reaction = MLSReactionModel(
            messageID: "msg-1",
            conversationID: "convo-1",
            currentUserDID: "user-1",
            actorDID: "actor-1",
            emoji: "❤️",
            action: "add",
            timestamp: Date()
        )
        
        try await storage.saveReaction(reaction, database: dbQueue)
        
        // Verify regular save
        let reacts = try await dbQueue.read { db in
            try MLSReactionModel.fetchAll(db)
        }
        XCTAssertEqual(reacts.count, 1)
        XCTAssertEqual(reacts.first?.emoji, "❤️")
        
        // Verify no orphans
        let orphans = try await dbQueue.read { db in
            try MLSOrphanedReactionModel.fetchAll(db)
        }
        XCTAssertEqual(orphans.count, 0)
    }
}
