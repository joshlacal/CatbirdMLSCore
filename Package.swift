// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import Foundation
import PackageDescription

let packageDir = URL(fileURLWithPath: #file).deletingLastPathComponent().path
let localFrameworkPath = "\(packageDir)/Sources/CatbirdMLSFFI.xcframework"
let useLocalBinary = FileManager.default.fileExists(atPath: localFrameworkPath)
    || FileManager.default.fileExists(atPath: "Sources/CatbirdMLSFFI.xcframework")
    || ProcessInfo.processInfo.environment["CATBIRD_USE_LOCAL_FFI"] == "1"

let ffiTarget: Target = useLocalBinary
    ? .binaryTarget(
        name: "CatbirdMLSFFI",
        path: "Sources/CatbirdMLSFFI.xcframework"
    )
    : .binaryTarget(
        name: "CatbirdMLSFFI",
        url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.4.1/CatbirdMLSFFI.xcframework.zip",
        checksum: "a9490e656052177d6e24d338a59fc3b02b9e544ce2e3d5eb2cebab26a3b37be0"
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
        )
    ],
    dependencies: [
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.0.0"),
        .package(url: "https://github.com/joshlacal/Petrel.git", from: "1.0.7"),
        .package(path: "../PetrelCatbird")
    ],
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
        )
    ]
)
