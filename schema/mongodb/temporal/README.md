# Temporal MongoDB Schema

This directory contains versioned MongoDB DDL artifacts for Temporal default persistence and visibility.

## Layout

```
schema/mongodb/temporal/
├── README.md              # This file
└── versioned/
    ├── v1.0/
    │   ├── manifest.json  # Initial default persistence schema
    │   └── schema.json
    └── v1.1/
        ├── manifest.json  # Native visibility schema
        └── schema.json
```

## Conventions

- Each version has a manifest and one or more command files applied in manifest order.
- New versions use a new semantic-version directory and are applied in ascending version order.
- Collections and indexes defined here mirror the names used in `common/persistence/mongodb/*`.

## Applying The Schema

```
make temporal-mongodb-tool
./temporal-mongodb-tool -u temporal --pw temporal --db temporal setup-schema -v 0.0
./temporal-mongodb-tool -u temporal --pw temporal --db temporal update-schema
./temporal-mongodb-tool -u temporal --pw temporal --db temporal version
```

Use `--endpoint` for comma-separated hosts, `--replica-set` for the replica-set name, `--tls` plus the TLS file flags for encrypted deployments, and `update-schema --version` to stop at a specific version. Schema setup and updates never drop collections. Existing collection creation is idempotent, while conflicting indexes and malformed or missing versions fail explicitly.
