import PetrelCatbird

/// Projects an encrypted MLS payload onto the generated server-view
/// discriminator. The discriminator describes the outer MLS message class,
/// not the payload's inner `MLSMessageType`; every known payload here is an
/// application message. Commit messages are received from the server and use
/// `.value_commit` directly from the generated DTO.
public enum MLSMessageViewProjection {
  public static func viewType(
    for payload: MLSMessagePayload
  ) throws -> BlueCatbirdMlsChatDefs.MessageViewMessageType {
    guard payload.messageType != .unknown else {
      throw MLSMessageViewProjectionError.unsupportedPayloadType(payload.messageType)
    }
    return .value_app
  }

  public static func rawType(
    _ type: BlueCatbirdMlsChatDefs.MessageViewMessageType?
  ) -> String {
    type?.rawValue ?? BlueCatbirdMlsChatDefs.MessageViewMessageType.value_app.rawValue
  }
}

public enum MLSMessageViewProjectionError: Error, Equatable {
  case unsupportedPayloadType(MLSMessageType)
}
