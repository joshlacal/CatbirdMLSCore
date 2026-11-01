// swift-tools-version: 6.0
// Clean-slate CatbirdMLSCore manifest: thin Swift wrapper over Rust-owned SQLCipher + OpenMLS SDK.
// Zero GRDB dependency (I1 / G1).

import Foundation
import PackageDescription

let environment = ProcessInfo.processInfo.environment
let packageRoot = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
let siblingRoot = packageRoot.deletingLastPathComponent()

func siblingPathIfAvailable(_ name: String) -> String? {
  let candidate = siblingRoot.appendingPathComponent(name).standardizedFileURL
  let manifest = candidate.appendingPathComponent("Package.swift")
  var isDirectory: ObjCBool = false
  guard FileManager.default.fileExists(atPath: candidate.path, isDirectory: &isDirectory),
        isDirectory.boolValue,
        FileManager.default.fileExists(atPath: manifest.path),
        candidate.resolvingSymlinksInPath() == candidate
  else {
    return nil
  }
  return candidate.path
}

let forceRemote = environment["CATBIRD_MLS_LOCAL_INTEGRATION"] == "0"
let sdkDependencies: [Package.Dependency]
if !forceRemote,
   let localPetrel = siblingPathIfAvailable("Petrel"),
   let localPetrelCatbird = siblingPathIfAvailable("PetrelCatbird")
{
  sdkDependencies = [
    .package(path: localPetrel),
    .package(path: localPetrelCatbird),
  ]
} else {
  sdkDependencies = [
    .package(
      url: "https://github.com/joshlacal/Petrel.git",
      revision: "92b05b81966c07a2a7d3d96a68f9230dd7787c5d"
    ),
    .package(
      url: "https://github.com/joshlacal/PetrelCatbird.git",
      revision: "3208d02e38e9b7b176d8c2cf46d1e92feb555974"
    ),
  ]
}

let localFFI = "Sources/CatbirdMLSFFI.xcframework"
let localFFIPath = packageRoot.appendingPathComponent(localFFI).standardizedFileURL
let simulatorFFI = "Sources/CatbirdMLSFFISimulator.xcframework"
let simulatorFFIPath = packageRoot.appendingPathComponent(simulatorFFI).standardizedFileURL

func hasVerifiedProtocolMarker(at frameworkURL: URL) -> Bool {
  let marker = frameworkURL.appendingPathComponent("labyrinth-space-protocol.json")
  guard let data = try? Data(contentsOf: marker),
        let value = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
        value["protocolVersion"] as? Int == 20261023,
        let features = value["features"] as? [String],
        features.contains("labyrinth"),
        let fingerprint = value["sourceFingerprint"] as? String,
        fingerprint.count == 64
  else {
    return false
  }
  return true
}

let selectedLocalRelativeFFI: String? = {
  if let explicit = environment["CATBIRD_MLS_FFI_PATH"] {
    let url = URL(fileURLWithPath: explicit).standardizedFileURL
    if url == simulatorFFIPath, hasVerifiedProtocolMarker(at: simulatorFFIPath) {
      return simulatorFFI
    }
    if url == localFFIPath, hasVerifiedProtocolMarker(at: localFFIPath) {
      return localFFI
    }
    fatalError("CATBIRD_MLS_FFI_PATH must point to a verified clean-slate CatbirdMLSFFI xcframework")
  }
  if !forceRemote, hasVerifiedProtocolMarker(at: localFFIPath) {
    return localFFI
  }
  if !forceRemote, hasVerifiedProtocolMarker(at: simulatorFFIPath) {
    return simulatorFFI
  }
  return nil
}()

let ffiTarget: Target = selectedLocalRelativeFFI.map {
  .binaryTarget(name: "CatbirdMLSFFI", path: $0)
} ?? .binaryTarget(
  name: "CatbirdMLSFFI",
  url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.5.21/CatbirdMLSFFI.xcframework.zip",
  checksum: "ee9bbc95e2aeed94e1cf400eac202fb35f1be15f8e2668f7980e7e818a92bf72"
)

let package = Package(
  name: "CatbirdMLSCore",
  platforms: [
    .iOS(.v18),
    .macOS(.v15),
  ],
  products: [
    .library(
      name: "CatbirdMLSCore",
      targets: ["CatbirdMLSCore"]
    ),
  ],
  dependencies: sdkDependencies,
  targets: [
    .target(
      name: "CatbirdMLSCore",
      dependencies: [
        "CatbirdMLS",
        "Petrel",
        .product(name: "PetrelCatbird", package: "PetrelCatbird"),
      ],
      swiftSettings: [
        .define("CATBIRD_MLS_CLEAN_SLATE"),
        // Keep Swift 5 mode for UniFFI bindings compatibility
        .swiftLanguageMode(.v5),
      ],
      linkerSettings: [
        .linkedFramework("Security"),
        .linkedFramework("SystemConfiguration"),
      ]
    ),
    .target(
      name: "CatbirdMLS",
      dependencies: ["CatbirdMLSFFI"],
      path: "Sources/CatbirdMLS",
      swiftSettings: [
        .swiftLanguageMode(.v5),
      ]
    ),
    ffiTarget,
    .testTarget(
      name: "CatbirdMLSCoreTests",
      dependencies: ["CatbirdMLSCore", "CatbirdMLS"],
      swiftSettings: [
        .define("CATBIRD_MLS_CLEAN_SLATE"),
      ]
    ),
  ]
)
