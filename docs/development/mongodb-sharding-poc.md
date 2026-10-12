# Bucketed MongoDB sharding

This POC changes persistence placement, not Temporal's logical History ownership
or its workflow interfaces. It requires a fresh database; there is no migration,
dual-read path or metadata repair for the previous collection layout.

## History placement

The storage model is `bucketed_atomic_v1`, with 64 buckets per logical History
shard and sharding plan 5. Namespace plus workflow ID selects a stable bucket;
run ID is excluded so continue-as-new keeps the same placement.

Fence, workflow state, current pointer and History tasks share `history_records`
with shard key `{shard_id: 1, storage_bucket: 1}`. Different workflows within a
logical shard can reach different MongoDB shards. A single workflow's core
records are not split across buckets. History branches and nodes retain their
independent `tree_id` placement, so a workflow transaction may still span shards.

Each write locks the selected bucket's ownership fence inside the transaction.
Ownership takeover updates all 64 fences atomically. Missing or incompatible
fences fail admission rather than being reconstructed.

History task reads merge ordered results across buckets. Category boundaries,
pagination and single/range acknowledgements retain the existing persistence
contract. Task IDs still depend on Temporal's global allocator; a MongoDB unique
index is local to the full shard-key prefix, not a substitute for that allocator.

## Matching placement

Queue fences and backlog share `matching_records` with shard key
`{namespace_id: 1, task_queue: 1, task_type: 1}`. Fair subqueues remain with their
physical queue. One physical queue stays indivisible; partition task queues to
spread an exceptionally hot queue.

## Schema and verification

Schema installation records the storage model and bucket count. Server startup
rejects incompatible model/topology metadata. Record-kind partial indexes cover
state/current identities without indexing unrelated task/fence records.

Direct connector tests cover stable bucket routing, ownership takeover,
stale-writer refusal, colocated records and ordered task/Matching operations.
Use the existing MongoDB integration fixtures and `test_dep,integration` tags
for actual database checks. Local conformance is not measured scaling, active
growth, replicated capacity, failover or soak qualification.

Deployment, image packaging, workload replay and performance experiments belong
to the external testing harness/project evidence, not this sharding-key change.
