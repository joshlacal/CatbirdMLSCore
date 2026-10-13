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
            revision: "329b5fbe412286ff4c094246b6f30c2989e58f94"
        ),
        .package(
            url: "https://github.com/joshlacal/PetrelCatbird.git",
            revision: "ff98deedc748777537f912fa4dcd208f9bbc840e"
        ),
    ]

// A local native framework is selected only through the development wrapper.
// Merely leaving an ignored framework in Sources must not change package identity.
let localFFI = "Sources/CatbirdMLSFFI.xcframework"
let localFFIPath = packageRoot.appendingPathComponent(localFFI).standardizedFileURL
let selectedFFIPath = environment["CATBIRD_MLS_FFI_PATH"].map {
    URL(fileURLWithPath: $0).standardizedFileURL
}
if selectedFFIPath != nil && !useLocalIntegration {
    fatalError("CATBIRD_MLS_FFI_PATH requires CATBIRD_MLS_LOCAL_INTEGRATION=1")
}
if let selectedFFIPath {
    guard selectedFFIPath == localFFIPath,
          selectedFFIPath.resolvingSymlinksInPath() == selectedFFIPath,
          FileManager.default.fileExists(atPath: selectedFFIPath.path) else {
        fatalError("CATBIRD_MLS_FFI_PATH must select the exact local framework at \(localFFIPath.path)")
    }
}
let ffiTarget: Target = selectedFFIPath != nil
    ? .binaryTarget(name: "CatbirdMLSFFI", path: localFFI)
    : .binaryTarget(
        name: "CatbirdMLSFFI",
        url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.5.19/CatbirdMLSFFI.xcframework.zip",
        checksum: "95ec7be2f2c415a152bdbeff5b739aa4e568fc460f3ab9c3f41d333d117b6349"
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
            swiftSettings: [
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
            path: "Sources/CatbirdMLS",
            swiftSettings: [
                // Keep Swift 5 mode for auto-generated UniFFI bindings
                // UniFFI generates mutable global state that isn't Swift 6 compatible
                .swiftLanguageMode(.v5)
            ]
        ),
        ffiTarget,
        .testTarget(
            name: "CatbirdMLSCoreTests",
            dependencies: ["CatbirdMLSCore"]
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
