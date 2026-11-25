# CLP Package CLI

This Rust crate holds the CLP package's command-line tools for the clp-s metadata database:

* `db-table-creator`: creates the shared metadata tables and the `default` dataset.
* `archive-manager`: finds archives, and deletes them from the metadata database and storage.
* `dataset-manager`: lists datasets, and marks them as deleted.

Each tool reads the package configuration from `--config`, and the database credentials from
`CLP_DB_USER` and `CLP_DB_PASS`. They only support the clp-s storage engine.

## Testing

The database tests are ignored by default. To run them, point `CLP_METADATA_DB_TEST_SERVER_URL` at
a MariaDB or MySQL server whose user can create and drop databases:

```shell
CLP_METADATA_DB_TEST_SERVER_URL=mysql://<user>:<password>@127.0.0.1:3306 \
    cargo test -p clp-package-cli -- --include-ignored
```

Each test recreates its own database, named `clp_package_cli_test_<test>`.
