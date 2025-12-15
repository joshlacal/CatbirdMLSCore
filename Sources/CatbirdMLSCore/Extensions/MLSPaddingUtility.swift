//
//  MLSPaddingUtility.swift
//  CatbirdMLSCore
//
//  Utility for stripping padding from MLS messages.
//  This is shared between the main app and the Notification Service Extension.
//
//  Padding Format: [4-byte BE length][actual MLS message][zero padding...]
//  Bucket sizes: 256, 512, 1024, 2048, 4096, 8192, or multiples of 8192
//

import Foundation

/// Utility for handling MLS message padding
///
/// MLS messages may be padded to fixed bucket sizes for traffic analysis resistance.
/// This utility strips the padding before passing ciphertext to the MLS deserializer.
public enum MLSPaddingUtility {
  
  /// Strip padding from received MLS ciphertext if present
  ///
  /// The padding format is: [4-byte BE length][ciphertext][zero padding...]
  /// If the data doesn't appear to be padded, it's returned unchanged.
  ///
  /// - Parameter data: Possibly padded MLS ciphertext
  /// - Returns: Original ciphertext without padding envelope
  public static func stripPaddingIfPresent(_ data: Data) -> Data {
    guard data.count >= 4 else {
      return data
    }

    // Read the 4-byte big-endian length prefix
    let actualLength = Int(
      data.prefix(4).withUnsafeBytes {
        $0.load(as: UInt32.self).bigEndian
      })

    // Validate the length is plausible
    guard actualLength > 0, actualLength <= data.count - 4 else {
      // Length doesn't make sense - assume not padded
      return data
    }

    let startIndex = 4
    let endIndex = 4 + actualLength
    let slice = data[startIndex..<endIndex]

    // Validate that the stripped payload begins with a valid MLS wire format byte.
    // MLS wire format bytes are in range 0-4:
    // - 0: mls10_public_message (not used in MLS 1.0 final)
    // - 1: mls10_private_message
    // - 2: mls10_welcome
    // - 3: mls10_group_info
    // - 4: mls10_key_package
    if let wireFormat = slice.first, (0...4).contains(wireFormat) {
      return Data(slice)
    }

    // Wire format byte is invalid - this might be:
    // 1. An unpadded message where first 4 bytes happen to look like a valid length
    // 2. A corrupted message
    // Return original data to let the MLS library handle it
    return data
  }
  
  /// Check if data appears to be padded
  ///
  /// - Parameter data: Possibly padded MLS ciphertext
  /// - Returns: true if the data appears to have padding envelope
  public static func isPadded(_ data: Data) -> Bool {
    guard data.count >= 4 else {
      return false
    }

    let actualLength = Int(
      data.prefix(4).withUnsafeBytes {
        $0.load(as: UInt32.self).bigEndian
      })

    guard actualLength > 0, actualLength <= data.count - 4 else {
      return false
    }

    let startIndex = 4
    let slice = data[startIndex...]
    
    if let wireFormat = slice.first, (0...4).contains(wireFormat) {
      return true
    }

    return false
  }
}
