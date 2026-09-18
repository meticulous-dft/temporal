# MongoDB Persistence Adoption Baseline

## Status

The MongoDB default persistence backend passes the shared persistence suites and the planned timeout, service-restart, replica-set stepdown, and schema-lifecycle qualifications on Temporal Server `release/v1.31.x`. Visibility remains PostgreSQL or Elasticsearch. These checks establish the implementation baseline; they do not by themselves establish a production support commitment.

## Source and target

- Target: Temporal Server `release/v1.31.x` at `57c5abf712d51a4aaa88f78ea5c93ec17c920ac9` (`v1.31.2-2`)
- Donor: `fork/mongo/v1.31.0-151.5-dev` at `d2f653e6022793558d602065b09f49cc3791b31c`
- Visibility: PostgreSQL 12 plugin; MongoDB visibility is excluded

The donor implementation was extracted onto the stable release instead of retaining the donor branch history as the product base.

## Included scope

- MongoDB configuration and datastore factory wiring
- Task and fair-task stores
- Shard, namespace metadata, execution, and history stores
- Queue and QueueV2 stores
- Cluster metadata and Nexus endpoint stores
- Versioned MongoDB schema setup, inspection, compatibility checking, and ordered updates
- Local authenticated single-node replica-set configuration
- MongoDB persistence test registration and fixtures
- MongoDB driver dependency and persistence metrics

The port directly registers MongoDB as a core `DataStore` variant. Extracting it behind `WithCustomDataStoreFactory` remains a possible packaging step after the correctness spike; doing that during adoption would mix interface extraction with validation of the donor implementation.

## Excluded donor drift

The following donor changes were deliberately not carried:

- MongoDB visibility implementation and query conversion
- visibility-specific server, telemetry, and fault-injection plumbing
- CI, release, image-tag, and Docker publication changes
- unrelated upstream tests and documentation
- donor-specific build and workflow changes not required by MongoDB default persistence

MongoDB visibility, sharding, Temporal multi-cluster/XDC, release automation, and a general support commitment remain non-goals for this baseline.

## Temporal 1.31.2 adaptation

- Implemented the required fair-task store alongside the classic task store. Their task-queue metadata uses separate v1 and v2 identities so migration can hold independent leases.
- Adapted current-execution identity to include the CHASM archetype ID required by the v1.31 execution-store contract.
- Registered MongoDB in the current persistence test routing and preserved MongoDB configuration when test defaults are applied.
- Reinitialized protocol-buffer assertions in the shared execution mutable-state suite so the MongoDB suite can reuse the current v1.31 tests.

All datastore factory methods required by the current `release/v1.31.x` head compile through the Temporal server wiring.

## Defects found during adoption

Three donor defects were exposed and corrected:

1. Explicit unique `_id` index declarations for Nexus collections conflicted with MongoDB's built-in `_id` indexes.
2. A unique task-queue lookup index prevented the v1 and v2 metadata documents from coexisting. This caused repeated `task queue already exists` failures in Matching during live startup. The index is now non-unique; document `_id` remains the uniqueness boundary.
3. A read-only shard range check allowed an old owner to commit workflow state after ownership advanced. Workflow transactions now conditionally modify the shard document to acquire a write lock; the deterministic takeover test verifies that stale execution, current-execution, history, and task writes all remain absent.

After applying the corrected schema to an empty database, live startup created both v1 and v2 task-queue records and the repeated Matching failure disappeared.

## Verification

The following checks passed on September 9, 2026:

```text
go test -tags test_dep -count=1 ./common/config ./common/persistence/mongodb ./common/persistence/client
go test -tags test_dep -count=1 ./common/persistence/tests -run MongoDB
go test -tags test_dep -run '^$' ./common/persistence/... ./cmd/server ./temporal
make temporal-server
GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false make lint-code
```

The selected MongoDB persistence package completed in 15.759 seconds. It reported two expected skips and no unexplained skips:

- `TestListConcreteExecutions`: Cassandra-only contract test
- `TestRenameNamespaceSQL`: SQL-only contract test

Schema bootstrap succeeded on an empty authenticated MongoDB 7.0 replica set. A complete server then started with MongoDB default persistence and PostgreSQL visibility, remained free of post-start error logs during a 30-second observation window, and returned the `temporal-system` namespace from `GET /api/v1/namespaces`.

The repository's default `make lint-code` baseline expects a local `main` revision, which this tag-based worktree does not have. Running it without an explicit baseline exposes thousands of pre-existing upstream findings. The changed-lines lint and vet gate above passed with `HEAD` as the baseline and automatic fixes disabled.

### Test coverage follow-up

The feature-level comparison was repeated on September 12, 2026. MongoDB now registers all 15 shared persistence suites registered by Cassandra, including `HistoryV2PersistenceSuite`. The shared `ListConcreteExecutions` scenario also runs against MongoDB instead of relying only on its store-level unit test.

Those additions exposed and fixed two integration defects: the MongoDB test cluster omitted Temporal's required transaction-size limit, and `ListConcreteExecutions` returned fields outside the manager contract. The full MongoDB shared suite now has one intentional skip, `TestRenameNamespaceSQL`; MongoDB rename behavior is covered by the generic and non-SQL rename scenarios.

The shard and task-queue ownership races are covered separately because they require deterministic transaction interleaving:

```text
go test -tags 'test_dep integration' -count=1 ./common/persistence/mongodb -run '^(TestExecutionStoreRejectsStaleShardOwner|TestTaskStoreRejectsStaleTaskQueueOwner)$'
```

The task-queue takeover test confirms that `CreateTasks` already uses its conditional task-queue metadata update as a write fence: after the lease advances, the old owner receives `ConditionFailedError`, inserts no tasks, and cannot overwrite the new owner's metadata.

MongoDB client construction now applies the configured read preference and write concern, defaulting empty values to `primary` and `majority`. Invalid values fail factory initialization before connecting. Transactions use explicit `snapshot` read concern, `primary` read preference, and the configured write concern through session defaults; the integration test verifies that transactions remain primary-routed when the client uses `primaryPreferred`.

Transaction retry qualification uncovered error translation inside callbacks that discarded MongoDB retry labels. MongoDB operation failures reachable from every transaction family now retain their original cause while continuing to implement Temporal's `Unavailable` service contract. The local replica-set harness enables `failCommand` for two deterministic scenarios:

```text
go test -tags 'test_dep integration' -count=1 ./common/persistence/mongodb -run '^(TestCreateTasksRetriesTransientTransactionError|TestCreateTasksRetriesUnknownCommitResult|TestQueueStoreRetriesTransientTransactionError)$'
```

Injected `TransientTransactionError` failures replay both the task store's direct transaction callback and the shared transaction helper used by execution, queue, and Nexus stores. Each path commits exactly one record. A dropped response to the first `commitTransaction` retries only the commit, does not replay the callback, and also leaves exactly one task.

## Dependency delta

The port adds `go.mongodb.org/mongo-driver v1.17.6` and its five transitive modules: `montanaflynn/stats`, `xdg-go/pbkdf2`, `xdg-go/scram`, `xdg-go/stringprep`, and `youmark/pkcs8`.

## Failure and lifecycle qualification

The three planned follow-up gates passed locally on September 12, 2026:

1. A transaction forced past its caller deadline aborted without persisting a partial task, and the same store completed a subsequent transaction. Thirty-two workflows blocked in activities also completed with exact results after both History and Matching were stopped and reconstructed.
2. The complete shared MongoDB persistence suite passed against a three-member replica set. A sustained workload then completed 801 unique transactional task writes while forcing four primary elections. Retry handling checked each uncertain write by exact task identity, and the final collection cardinality was exactly 801.
3. `temporal-mongodb-tool` now initializes, inspects, and updates schema metadata. Updates are loaded and applied in semantic-version order, record manifest hashes in `schema_update_history`, resume after a partially applied version, reject missing targets and manifest/version mismatches, and fail server startup when the installed schema is too old or requires a newer server.

The qualification commands were:

```text
MONGODB_SEEDS=127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019 \
  go test -tags 'test_dep integration' -count=1 ./common/persistence/mongodb

MONGODB_SEEDS=127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019 \
  go test -tags 'test_dep integration' -count=1 ./common/persistence/tests -run '^TestMongoDB'

MONGODB_SEEDS=127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019 \
  go test -tags 'test_dep integration' -count=1 ./tools/mongodb

MONGODB_SEEDS=127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019 \
  go test -tags 'test_dep integration' -count=1 ./tests \
    -run '^TestMongoDBPersistenceQualificationSuite$' \
    -persistenceType nosql -persistenceDriver mongodb
```

Operational soak testing, backup/restore validation, sharded-cluster qualification, Temporal multi-cluster/XDC qualification, and release automation remain separate follow-up work.
