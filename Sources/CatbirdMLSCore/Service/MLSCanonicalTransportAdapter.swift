import CatbirdMLS
import Foundation

/// Narrow iOS seam for the Rust clean-chat transport.
///
/// Petrel owns every request and response DTO. This adapter only turns those
/// generated Codable values into the bytes expected by UniFFI and decodes the
/// bytes returned by Rust; it does not define a second wire schema or execute
/// HTTP itself. Uploads and subscriptions remain deliberately outside this
/// seam until their dedicated platform transports are migrated.
public enum MLSCanonicalTransportAdapter {
  public static func prepare<Input: Encodable>(
    auth: CleanChatAuthContextFfi,
    operation: CleanChatOperationFfi,
    input: Input
  ) throws -> CleanChatPreparedRequestFfi {
    let requestJSON = try JSONEncoder().encode(input)
    return try prepareCleanChatRequest(
      auth: auth,
      operation: operation,
      requestJson: requestJSON
    )
  }

  public static func decode<Response: Decodable>(
    _ response: Data,
    operation: CleanChatOperationFfi,
    as _: Response.Type = Response.self
  ) throws -> Response {
    let canonicalJSON = try decodeCleanChatResponse(
      operation: operation,
      responseJson: response
    )
    return try JSONDecoder().decode(Response.self, from: canonicalJSON)
  }

  public static func decodeBlob(_ response: Data) throws -> Data {
    try decodeCleanChatBlob(responseBytes: response)
  }
}
