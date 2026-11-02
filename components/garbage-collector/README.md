# Garbage Collector

This Rust component contains the CLP package's garbage collectors for the clp-s storage engine:
`archive-garbage-collector`, which deletes the archives whose datasets' retention periods have
expired, and reclaims the metadata of deleted datasets.
