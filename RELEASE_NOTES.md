# MLS Chat v2 device lifecycle release

This release pairs the Swift API with the native MLS library used by device admission, durable leave, and canonical message synchronization. Consumers must use the released Package.swift revision as a unit; mixing generated Swift with an earlier native archive is unsupported.

Existing direct conversations can be adopted on a device that needs admission. Pending invitations require an explicit decision. Leaving a conversation retains readable local history and exposes pending, removed, and closed states consistently. Welcome processing and its acknowledgement are durable across interruption. Canonical stream events use the same Rust projection as synchronization, and fresh inventory requests share bounded retry handling.

## Verification carried forward from the source-identical integration

- Core:279 selected tests passed, with the full package and test target compiled.
- iOS:38 selected app tests passed; simulator and generic-device builds passed.
- Live multi-device checks covered direct-conversation adoption, invitation acceptance, account-wide leave, restart, and ordered delivery through subscription rotation.
- The release workflow additionally rebuilds the native framework for all seven Apple target architectures, checks the generated Swift binding against the committed source, and records exact dependency revisions.

These counts refer to the selected suites. Four unfiltered Core test failures were reproduced on the preceding baseline. A fresh device cannot recover historical plaintext automatically. A leafless group may need another active member to establish current account membership before an interrupted leave can finish safely.
