// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription
import Foundation

let environment = ProcessInfo.processInfo.environment
let localIntegrationValue = environment["CATBIRD_MLS_LOCAL_INTEGRATION"] ?? "0"
guard localIntegrationValue == "0" || localIntegrationValue == "1" else {
    fatalError("CATBIRD_MLS_LOCAL_INTEGRATION must be exactly 0 or 1")
}
let useLocalIntegration = localIntegrationValue == "1"
let packageRoot = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
let siblingRoot = packageRoot.deletingLastPathComponent()

func exactSibling(_ name: String) -> String {
    let candidate = siblingRoot.appendingPathComponent(name).standardizedFileURL
    let expectedManifest = candidate.appendingPathComponent("Package.swift")
    var isDirectory: ObjCBool = false
    guard FileManager.default.fileExists(atPath: candidate.path, isDirectory: &isDirectory),
          isDirectory.boolValue,
          FileManager.default.fileExists(atPath: expectedManifest.path) else {
        fatalError("Local integration requires the exact sibling package at \(candidate.path)")
    }
    guard candidate.resolvingSymlinksInPath() == candidate else {
        fatalError("Local integration rejects symlinked sibling package: \(candidate.path)")
    }
    return candidate.path
}

let sdkDependencies: [Package.Dependency] = useLocalIntegration
    ? [
        .package(path: exactSibling("Petrel")),
        .package(path: exactSibling("PetrelCatbird")),
    ]
    : [
        .package(
            url: "https://github.com/joshlacal/Petrel.git",
            revision: "e115236537041a900ec971be05185a70cca73e8a"
        ),
        .package(
            url: "https://github.com/joshlacal/PetrelCatbird.git",
            revision: "8672c80ffeb3b9cfbdb6bd98f66efa09b13fe0db"
        ),
    ]

// A local native framework is selected only through the development wrapper.
// Merely leaving an ignored framework in Sources must not change package identity.
let localFFI = "Sources/CatbirdMLSFFI.xcframework"
let localFFIPath = packageRoot.appendingPathComponent(localFFI).standardizedFileURL
let simulatorFFI = "Sources/CatbirdMLSFFISimulator.xcframework"
let simulatorFFIPath = packageRoot.appendingPathComponent(simulatorFFI).standardizedFileURL
let selectedFFIPath = environment["CATBIRD_MLS_FFI_PATH"].map {
    URL(fileURLWithPath: $0).standardizedFileURL
}
if selectedFFIPath != nil && !useLocalIntegration {
    fatalError("CATBIRD_MLS_FFI_PATH requires CATBIRD_MLS_LOCAL_INTEGRATION=1")
}
if let selectedFFIPath {
    guard selectedFFIPath == localFFIPath || selectedFFIPath == simulatorFFIPath,
          selectedFFIPath.resolvingSymlinksInPath() == selectedFFIPath,
          FileManager.default.fileExists(atPath: selectedFFIPath.path) else {
        fatalError("CATBIRD_MLS_FFI_PATH must select an exact local release or simulator framework")
    }
    // The public v1.5.21 artifact predates sequenced conversation Spaces and
    // the accompanying deletion/key-authority fixes. Merely copying that old
    // framework into Sources must not enable the new platform callback.
    let marker = selectedFFIPath.appendingPathComponent("labyrinth-space-protocol.json")
    guard let data = try? Data(contentsOf: marker),
          let value = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          value["protocolVersion"] as? Int == 20261023,
          value["artifactScope"] as? String == (selectedFFIPath == simulatorFFIPath
            ? "ios-arm64-simulator-only" : "apple-all-targets"),
          let features = value["features"] as? [String], features.contains("labyrinth"),
          let fingerprint = value["sourceFingerprint"] as? String,
          fingerprint.count == 64,
          fingerprint.allSatisfy({ $0.isHexDigit && !$0.isUppercase }) else {
        fatalError("Local Spaces FFI requires the verified rebuild marker at \(marker.path)")
    }
}
let ffiTarget: Target = selectedFFIPath != nil
    ? .binaryTarget(name: "CatbirdMLSFFI", path: selectedFFIPath == simulatorFFIPath ? simulatorFFI : localFFI)
    : .binaryTarget(
        name: "CatbirdMLSFFI",
        url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.5.21/CatbirdMLSFFI.xcframework.zip",
        checksum: "ee9bbc95e2aeed94e1cf400eac202fb35f1be15f8e2668f7980e7e818a92bf72"
    )

let package = Package(
    name: "CatbirdMLSCore",
    platforms: [
        .iOS(.v18),
        .macOS(.v15)
    ],
    products: [
        .library(
            name: "CatbirdMLSCore",
            targets: ["CatbirdMLSCore"]
        ),
        .executable(
            name: "CatbirdMLSTestHost",
            targets: ["CatbirdMLSTestHost"]
        )
    ],
    dependencies: [
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.0.0"),
    ] + sdkDependencies,
    targets: [
        .target(
            name: "CatbirdMLSCore",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
                "CatbirdMLS",
                "CatbirdMLSFFI",
                "Petrel",
                .product(name: "PetrelCatbird", package: "PetrelCatbird")
            ],
            swiftSettings: (selectedFFIPath != nil ? [.define("CATBIRD_MLS_VERIFIED_SPACE_FFI")] : []) + [
                // Keep Swift 5 mode for UniFFI bindings compatibility
                // UniFFI generates mutable global state that isn't Swift 6 compatible
                .swiftLanguageMode(.v5)
            ],
            linkerSettings: [
                .linkedFramework("Security"),
                .linkedFramework("SystemConfiguration"),
            ]
        ),
        .target(
            name: "CatbirdMLS",
            dependencies: ["CatbirdMLSFFI"],
            path: selectedFFIPath != nil ? "Sources/CatbirdMLSSpaces" : "Sources/CatbirdMLS",
            swiftSettings: [
                // Keep Swift 5 mode for auto-generated UniFFI bindings
                // UniFFI generates mutable global state that isn't Swift 6 compatible
                .swiftLanguageMode(.v5)
            ]
        ),
        ffiTarget,
        .testTarget(
            name: "CatbirdMLSCoreTests",
            dependencies: ["CatbirdMLSCore"],
            swiftSettings: selectedFFIPath != nil ? [.define("CATBIRD_MLS_VERIFIED_SPACE_FFI")] : []
        ),
        .executableTarget(
            name: "CatbirdMLSTestHost",
            dependencies: [
                "CatbirdMLSCore",
                .product(name: "GRDB", package: "GRDB.swift")
            ],
            path: "Sources/CatbirdMLSTestHost",
            swiftSettings: [
                .swiftLanguageMode(.v5)
            ]
        )
    ]
)
