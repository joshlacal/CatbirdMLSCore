// swift-tools-version: 6.0
// Clean-slate CatbirdMLSCore manifest: thin Swift wrapper over Rust-owned SQLCipher + OpenMLS SDK.
// Zero GRDB dependency (I1 / G1).

#if canImport(CryptoKit)
import CryptoKit
#endif
import Foundation
import PackageDescription

let environment = ProcessInfo.processInfo.environment
let packageRoot = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
let siblingRoot = packageRoot.deletingLastPathComponent()

func siblingPathIfAvailable(_ name: String) -> String? {
  let candidate = siblingRoot.appendingPathComponent(name, isDirectory: true).standardizedFileURL
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
      revision: "74b89dfb58fca288ee2003177fe1b287dbaf7ee8"
    ),
    .package(
      url: "https://github.com/joshlacal/PetrelCatbird.git",
      revision: "746b107512b9d4b003598aadfd57b4c77a3d4c7c"
    ),
  ]
}

let localFFI = "Sources/CatbirdMLSFFI.xcframework"
let localFFIPath = packageRoot.appendingPathComponent(localFFI).standardizedFileURL
let simulatorFFI = "Sources/CatbirdMLSFFISimulator.xcframework"
let simulatorFFIPath = packageRoot.appendingPathComponent(simulatorFFI).standardizedFileURL

func readObject(_ file: URL) -> [String: Any]? {
  guard let data = try? Data(contentsOf: file) else { return nil }
  return (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
}

func digest(_ file: URL) -> String? {
  #if canImport(CryptoKit)
  guard let data = try? Data(contentsOf: file, options: .mappedIfSafe) else { return nil }
  return SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
  #else
  return nil
  #endif
}

let bindingHash = digest(packageRoot.appendingPathComponent("Sources/CatbirdMLS/CatbirdMLS.swift"))

func qualifiedLocalArtifact(_ url: URL, scope: String) -> Bool {
  let receiptPath = scope == "apple-all-targets" ? "DevelopmentIntegration/spaces-apple-artifact-manifest.json" : ".build/qualified-simulator-ffi.json"
  let receiptURL = packageRoot.appendingPathComponent(receiptPath)
  guard let receipt = readObject(receiptURL),
        let source = receipt["source"] as? [String: Any], let bundle = source["bundle_sha256"] as? String,
        let marker = readObject(url.appendingPathComponent("labyrinth-space-protocol.json")),
        marker["sourceFingerprint"] as? String == bundle, marker["artifactScope"] as? String == scope,
        marker["protocolVersion"] as? Int == 20261023, (marker["features"] as? [String])?.contains("labyrinth") == true,
        let bindingHash, marker["generatedSwiftSHA256"] as? String == bindingHash,
        let libraryHashes = marker["librarySHA256"] as? [String: String], !libraryHashes.isEmpty
  else { return false }
  guard let framework = receipt["framework"] as? [String: Any],
        let files = framework["files"] as? [String: [String: Any]],
        Set(libraryHashes.keys) == Set(files.keys.filter { $0.hasSuffix("/libCatbirdMLSFFI.a") })
  else { return false }
  return files.allSatisfy { path, record in
    guard let expected = record["sha256"] as? String else { return false }
    return digest(url.appendingPathComponent(path)) == expected
  }
}

let selectedLocalRelativeFFI: String? = {
  let candidates = [(localFFI, localFFIPath, "apple-all-targets"), (simulatorFFI, simulatorFFIPath, "ios-arm64-simulator-only")]
  if let explicit = environment["CATBIRD_MLS_FFI_PATH"] {
    let selected = URL(fileURLWithPath: explicit).standardizedFileURL
    guard let (name, url, scope) = candidates.first(where: { $0.1 == selected }), qualifiedLocalArtifact(url, scope: scope) else {
      fatalError("CATBIRD_MLS_FFI_PATH does not match the recorded native source, Swift bindings and library checksums. Rebuild the XCFramework and regenerate its Apple artifact manifest.")
    }
    return name
  }
  guard !forceRemote else { return nil }
  for (name, url, scope) in candidates where FileManager.default.fileExists(atPath: url.path) {
    guard qualifiedLocalArtifact(url, scope: scope) else {
      fatalError("Local XCFramework does not match the recorded native source, Swift bindings and library checksums. Rebuild and regenerate the Apple artifact manifest.")
    }
    return name
  }
  return nil
}()

func publishedArtifact() -> Target {
  let receipt = readObject(packageRoot.appendingPathComponent("Scripts/ffi-release-inputs.json"))
  guard let bindingHash, receipt?["generated_swift_sha256"] as? String == bindingHash else {
    fatalError("No published CatbirdMLSFFI release matches these Swift bindings. Supply the locally verified XCFramework or publish a matching binary release.")
  }
  return .binaryTarget(
    name: "CatbirdMLSFFI",
    url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.5.21/CatbirdMLSFFI.xcframework.zip",
    checksum: "ee9bbc95e2aeed94e1cf400eac202fb35f1be15f8e2668f7980e7e818a92bf72"
  )
}

let ffiTarget: Target = selectedLocalRelativeFFI.map {
  .binaryTarget(name: "CatbirdMLSFFI", path: $0)
} ?? publishedArtifact()

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
    .testTarget(
      name: "CatbirdMLSGolden50Tests",
      dependencies: ["CatbirdMLSCore", "CatbirdMLS"],
      swiftSettings: [
        .define("CATBIRD_MLS_CLEAN_SLATE"),
      ]
    ),
  ]
)
