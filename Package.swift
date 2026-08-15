// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

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
        .package(url: "https://github.com/joshlacal/Petrel.git", .upToNextMinor(from: "1.0.7")),
        .package(url: "https://github.com/joshlacal/PetrelCatbird.git", .upToNextMinor(from: "1.1.0"))
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
        .binaryTarget(
            name: "CatbirdMLSFFI",
            url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/ffi-w3-40ee04f1/CatbirdMLSFFI.xcframework.zip",
            checksum: "27c31f88757ace274d40e175efcdeae3e0606faa213d7a08c651bfa180c557ca"
        ),
        .testTarget(
            name: "CatbirdMLSCoreTests",
            dependencies: ["CatbirdMLSCore"]
        )
    ]
)
