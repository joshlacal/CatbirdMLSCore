# MLS Chat v2 device lifecycle release

This release pairs the Swift API with the native MLS library used by device admission, durable leave, and canonical message synchronization. It fixes the missing recovery, leave-presentation, availability, and canonical-event APIs seen when the app resolved Core v1.5.17. Consumers must use the released Package.swift revision as a unit; mixing generated Swift with an earlier native archive is unsupported.

Existing direct conversations can be adopted on a device that needs admission. Pending invitations require an explicit decision. Leaving a conversation retains readable local history and exposes pending, removed, and closed states consistently. Welcome processing and its acknowledgement are durable across interruption. Canonical stream events use the same Rust projection as synchronization, and fresh inventory requests share bounded retry handling.

## Release archive verification

The [immutable release build](https://github.com/joshlacal/CatbirdMLSCore/actions/runs/34004430583) passed the native build for all seven Apple target architectures, verified the four XCFramework variants and paired headers, and confirmed that generated Swift exactly matches the committed binding. The published SDK passed 16 tests, and Core built successfully against the newly built binary. The archive records all five source revisions, transitive Swift package revisions, and file checksums.

## Verification carried forward from the source-identical integration

- Core:279 selected tests passed, with the full package and test target compiled.
- iOS:38 selected app tests passed; simulator and generic-device builds passed.
- Live multi-device checks covered direct-conversation adoption, invitation acceptance, account-wide leave, restart, and ordered delivery through subscription rotation.

These counts refer to the selected suites. Four unfiltered Core test failures were reproduced on the preceding baseline. A fresh device cannot recover historical plaintext automatically. A leafless group may need another active member to establish current account membership before an interrupted leave can finish safely.
