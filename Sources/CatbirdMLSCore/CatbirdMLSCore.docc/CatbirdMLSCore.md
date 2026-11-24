# ``CatbirdMLSCore``

The Apple-side wrapper over the Rust `catbird-mls` SDK, shared by the Catbird app and its Notification Service Extension.

## Overview

Rust owns MLS state, SQLCipher storage, sync and Labyrinth backup. CatbirdMLSCore supplies the Apple side: Keychain storage, App Group paths, the Petrel-backed network transport, NSE push processing and Darwin wake-ups. There is no GRDB layer.

The UniFFI bindings live in the separate `CatbirdMLS` target and are not documented here. See the package README for layout, the shared app and NSE database, and build instructions.
