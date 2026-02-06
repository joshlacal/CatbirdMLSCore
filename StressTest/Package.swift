// swift-tools-version: 6.0
// MLS Stress Test Harness - headless CLI for stress testing CatbirdMLSCore

import PackageDescription

let package = Package(
  name: "MLSStressTest",
  platforms: [
    .macOS(.v14)  // macOS only for CLI testing
  ],
  products: [
    .executable(
      name: "mls-stress-test",
      targets: ["MLSStressTest"]
    )
  ],
  dependencies: [
    .package(path: "../")  // CatbirdMLSCore
  ],
  targets: [
    .executableTarget(
      name: "MLSStressTest",
      dependencies: [
        .product(name: "CatbirdMLSCore", package: "CatbirdMLSCore")
      ],
      swiftSettings: [
        .swiftLanguageMode(.v5)  // Match CatbirdMLSCore for UniFFI compatibility
      ]
    )
  ]
)
