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
        // Published Petrel, pinned by revision. A sibling `path:` dependency
        // builds against whichever line the neighbouring checkout is on, which
        // no manifest records and no other machine reproduces.
        .package(
            url: "https://github.com/joshlacal/Petrel.git",
            revision: "e457465cd286852155177f60ac9c5758bd4f6868"
        ),
        // Published PetrelCatbird, pinned by revision.
        .package(
            url: "https://github.com/joshlacal/PetrelCatbird.git",
            revision: "3f727bd92f281da1d7d2baa026a852a66771c18a"
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
        .binaryTarget(
            name: "CatbirdMLSFFI",
            url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.5.13/CatbirdMLSFFI.xcframework.zip",
            checksum: "e048b3f791f86b45cc819bdd94eb4e4993a776f52070ac46e59ac9c187559dc2"
        ),
        .testTarget(
            name: "CatbirdMLSCoreTests",
            dependencies: ["CatbirdMLSCore"]
        )
    ]
)
