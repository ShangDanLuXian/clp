# CLP Metadata DB

This Rust crate is the interface of the clp-s metadata tables that every dataset shares
(`clp_datasets`, `clp_archives`, `clp_long_span_archives` and `clp_column_metadata`).

## Testing

The database tests are ignored by default. To run them, point `CLP_METADATA_DB_TEST_SERVER_URL` at
a MariaDB or MySQL server whose user can create and drop databases:

```shell
CLP_METADATA_DB_TEST_SERVER_URL=mysql://<user>:<password>@127.0.0.1:3306 \
    cargo test -p clp-metadata-db -- --include-ignored
```

Each test recreates its own database, named `clp_metadata_db_test_<test>`.
