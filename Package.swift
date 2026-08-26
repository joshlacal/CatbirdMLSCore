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
        url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.5.6/CatbirdMLSFFI.xcframework.zip",
        checksum: "253ca12220a2ac146de6e93f186465d4cacf641e3cbbbbfcc5074b652a444828"
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
        // Published Petrel, pinned by revision. A sibling `path:` dependency
        // builds against whichever line the neighbouring checkout is on, which
        // no manifest records and no other machine reproduces.
        .package(
            url: "https://github.com/joshlacal/Petrel.git",
            revision: "77879aa416a0e0b3c072514d52b8ea0ade745a32"
        ),
        // Published PetrelCatbird, pinned by revision.
        .package(
            url: "https://github.com/joshlacal/PetrelCatbird.git",
            revision: "a4a0539c8e8aa85f6578b25ea030329d6b0890ed"
        )
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
