# MongoDB sharding POC candidate

This is a customer-evaluation candidate, not a production support or performance
claim. Use a fresh MongoDB 8.0 database behind mongos with replicated data shards.
Do not point this image at the old correctness soak or failed model databases.
There is no migration or implicit metadata repair. Do not change the History
shard count after initializing the database.

## Layout and limits

The model is `bucketed_atomic_v1`, with 64 storage buckets per logical History
shard and sharding plan 5. Workflow identity (namespace + workflow ID) selects a
stable bucket; run ID does not. Fence, state, current pointer and History task
records share `history_records` with key `{shard_id: 1, storage_bucket: 1}`.
History branches/nodes retain independent `tree_id` placement. Transactions remain
atomic; independent history can still require a distributed transaction.

Matching queue fences and backlog share `matching_records` with key
`{namespace_id: 1, task_queue: 1, task_type: 1}`. Fair subqueues stay with their
physical queue. One workflow and one physical Matching queue remain indivisible;
partition task queues to spread a hot queue. Ordered History task reads fan out
over buckets. Ownership takeover updates all 64 fences atomically. Task IDs retain
Temporal's global allocator invariant; MongoDB's unique index is placement-local.

Local conformance, stale-owner/rollback tests, controlled physical placement and
workflow service restart are separate from replicated capacity comparisons,
redistribution under load, failover and a 72-hour operational soak. Only results
with the same source, image, schema and configuration qualify this candidate.

## Load the delivered image

Verify the supplied archive checksums before loading the architecture-specific
Docker archive. Record the supplied source SHA-256, image ID, model/plan/bucket
labels, MongoDB version, database, History shard count and workload configuration.
The source fingerprint includes the actual modified and untracked source files;
the Git revision alone is not the candidate identity.

```bash
docker load --input temporal-mongodb-poc-arm64.tar
export POC_IMAGE='temporal-mongodb-poc:<supplied-source-prefix>-arm64'
docker image inspect --format '{{.Id}} {{json .Config.Labels}}' "$POC_IMAGE"
```

Use the `amd64` archive/tag on x86-64. No registry publication is implied by a
local image build. The image contains both server and schema tool, with embedded
schema assets; it runs as UID 1000.

## Fresh database bootstrap

Prepare private environment files, readable only by the operator. Do not commit
them or attach them to evidence. `MONGODB_URI` contains the mongos/SRV connection
string without an imposed replica-set name. Do not also set `MONGODB_SEEDS`.
Configure `MONGODB_DATABASE` to a new, separately labelled database and set
`MONGODB_USER`, `MONGODB_PASSWORD`, `MONGODB_AUTH_SOURCE=admin`, `MONGODB_TLS=true`.
Keep certificate/hostname verification enabled. TLS may be explicitly false only
for an isolated local fixture. Atlas requires network access and appropriate TLS.

Use a bootstrap principal authorized to create collections/indexes and install
the shard keys; keep its credentials separate from the runtime principal. Runtime
credentials need only the permissions required for Temporal persistence. Confirm
the selected database is empty before initialization. Never run `setup-schema`
against a populated or previously initialized database.

```bash
chmod 600 poc-schema.env poc-runtime.env
docker run --rm --env-file ./poc-schema.env \
  --entrypoint temporal-mongodb-tool "$POC_IMAGE" setup-schema --version 0.0
docker run --rm --env-file ./poc-schema.env \
  --entrypoint temporal-mongodb-tool "$POC_IMAGE" update-schema
docker run --rm --env-file ./poc-schema.env \
  --entrypoint temporal-mongodb-tool "$POC_IMAGE" version
```

`version` reports the numeric schema (1.1.0, minimum compatible 1.0). Inspect
`schema_version` separately with a read-only MongoDB tool: storage model must be
`bucketed_atomic_v1`, bucket count 64, topology `sharded`, sharding plan `5`.
Confirm actual collection shard keys, not just metadata. Installation and server
startup reject incompatible topology/model metadata. A failed bootstrap is not
permission to stamp a different model, delete data or retry silently.

## Start a private, single-container evaluation server

The bundled templated config defaults to 32 logical History shards, 50 connections,
primary reads and majority writes. Set `NUM_HISTORY_SHARDS` before initialization
and retain it for that database. The entrypoint discovers the container's broadcast
IP and binds services inside the container. Only the frontend needs exposure to
the customer SDK; do not expose History/Matching/membership ports publicly.

```bash
docker run --detach --name temporal-mongodb-poc \
  --env-file ./poc-runtime.env --env TEMPORAL_ALLOW_NO_AUTH=true \
  --publish 127.0.0.1:7233:7233 "$POC_IMAGE"
docker logs temporal-mongodb-poc
```

This command deliberately allows an unauthenticated Temporal API on the host's
loopback only. Use a private tunnel/network for remote evaluation or mount a
reviewed Temporal TLS/auth config instead; do not make this API public. MongoDB
credentials are not baked into the image. This is a single Temporal host, not HA.
The included dynamic configuration has no test-only performance/fairness overrides.

Connect the Temporal CLI/SDK to `127.0.0.1:7233`, create the evaluation namespace,
and run the customer's actual worker/workflows. Capture independent acknowledged
workflow IDs/results and replay checks. Stop/restart this same container with its
database retained, then require every acknowledged workflow to complete correctly.
Server startup or `cluster health` alone is not workload qualification. A workflow
failure, inconsistent result or missing acknowledgement must fail the evaluation.

## Qualification still required

Freeze this candidate before replicated fixed-capacity 1/2/3-shard comparisons.
Hold payload, workers, client/service capacity and workload mix fixed. Measure
throughput/latency, retry rate, backlog, client/service headroom, bytes/indexes and
populated-group ownership. Require balancing compliance and actual movement while
load is active. Distinguish automatic redistribution from controlled movement.
Run the bounded rehearsal before the 72-hour soak, then drain and independently
verify acknowledged work, task uniqueness, state/current/fences and replay.
No completed model screen, local split/move, or passing smoke replaces those gates.

## Rebuild from the frozen source

Use the repository's Go version and existing `disable_grpc_modules` build tag,
`CGO_ENABLED=0`, and the supplied source manifest/archive. Build each Linux
architecture's `cmd/server` and `cmd/tools/mongodb` into a fresh build context at
`build/<arch>/temporal-server` and `build/<arch>/temporal-mongodb-tool`. Copy only
the POC Dockerfile, existing entrypoint and the two POC config files, preserving
their repository-relative paths. Build from that clean context:

```bash
docker build --platform linux/arm64 \
  --file docker/targets/mongodb-poc.Dockerfile \
  --build-arg TARGETARCH=arm64 --build-arg ALPINE_TAG=3.23.5 \
  --build-arg SOURCE_SHA256="$POC_SOURCE_SHA256" \
  --tag "$POC_IMAGE" .
```

Keep the exact base-image identity, build logs, final image IDs and checksums.
Changing source, schema, config or image creates a new candidate and invalidates
reuse of qualification results for the old candidate.
