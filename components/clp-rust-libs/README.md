# CLP Rust Libraries

This directory contains the Rust library crates shared by CLP's Rust components.

A crate belongs here if other crates depend on it and it has no deployable artifact of its own.
Services and other deployables stay directly under `components/`.

Each crate is in its own subdirectory, and new crates must be added to the workspace `members` in
the root `Cargo.toml`.

## Crates

* [clp-metadata-db]: Operations on the clp-s metadata tables that every dataset shares in the CLP
  DB.
* [clp-rust-utils]: Common utilities used by other Rust components in the CLP package.

[clp-metadata-db]: clp-metadata-db/README.md
[clp-rust-utils]: clp-rust-utils/README.md
