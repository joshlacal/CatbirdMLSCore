import Foundation

/// A deterministic device record is append-only from the client's perspective:
/// an existing different key is a conflict, never permission to replace it.
internal enum MLSDeviceRecordPublication {
  enum Failure: LocalizedError {
    case invalidMaterial, conflictingRecord, unconfirmedPublication
    var errorDescription: String? {
      switch self {
      case .invalidMaterial:
        return "This device's signing identity is incomplete. Your existing keys have been kept."
      case .conflictingRecord:
        return "This device's published authorization uses a different key. Your saved keys have been kept."
      case .unconfirmedPublication:
        return "Device authorization could not be confirmed. Please try again."
      }
    }
  }

  static func ensure(
    userDid: String, deviceId: String, publicKey: Data,
    read: () async throws -> [MLSPublicPDSReader.AuthorizedDeviceRecord],
    create: () async throws -> Void
  ) async throws {
    guard let uuid = UUID(uuidString: deviceId), uuid.uuidString.lowercased() == deviceId,
          uuid.uuid.6 >> 4 == 4, uuid.uuid.8 & 0xc0 == 0x80,
          publicKey.count == 32 else { throw Failure.invalidMaterial }
    let uri = "at://\(userDid)/\(MLSPublicPDSReader.deviceCollection)/\(deviceId)"
    func matches(_ records: [MLSPublicPDSReader.AuthorizedDeviceRecord]) throws -> Bool {
      guard let record = records.first(where: { $0.uri == uri }) else { return false }
      guard record.publicKey == publicKey else { throw Failure.conflictingRecord }
      return true
    }
    if try matches(await read()) { return }
    do { try await create() }
    catch {
      let createError = error
      // A lost acknowledgement may follow a successful create. Only a fresh
      // matching repository record converts that ambiguity into success.
      if let records = try? await read(), (try? matches(records)) == true { return }
      throw createError
    }
    guard try matches(await read()) else { throw Failure.unconfirmedPublication }
  }
}
