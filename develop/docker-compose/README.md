# Temporal Server `docker compose` files for server development

These `docker compose` files run Temporal server development dependencies. Basically, they run everything you need to run
Temporal server besides a server itself which you suppose to run locally on the host in your favorite IDE or as binary.

You are not supposed to use these files directly. Please use [Makefile](../../Makefile) targets instead. To start dependencies:

```bash
make start-dependencies
```

To stop dependencies:

```bash
make stop-dependencies
```

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

## MongoDB replica set support

- The shared docker-compose definitions now start a single-node MongoDB replica set on `mongodb://temporal:temporal@localhost:27017/temporal?authSource=admin&replicaSet=rs0`.
- Transactions are enabled via `rs0`; the container health check initializes the replica set during startup.
- Use the `temporal` user (password `temporal`) when pointing Temporal Server at the local MongoDB instance. For the official Mongo image this user is created in the `admin` database, so clients typically need `authSource=admin`.
- This local test instance enables MongoDB test commands for deterministic transaction failure injection. Do not copy that setting to production deployments.
- Run `make install-schema-mongodb` before starting Temporal Server with `make start-mongodb`.

To run the shared persistence integration suites against this MongoDB instance (see `common/persistence/tests`), configure:

- `MONGODB_SEEDS` (default: `127.0.0.1:27017`; accepts comma-separated hosts)
- `MONGODB_PORT` (default: `27017`)
- `MONGODB_REPLICA_SET` (default: `rs0`)

The failure-qualification environment runs three replica-set members plus Elasticsearch:

```bash
make start-mongodb-qualification
export MONGODB_SEEDS=127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019
# Run MongoDB persistence and functional qualification commands.
make stop-mongodb-qualification
```

It uses ports 27017-27019 and 9200, so stop the regular dependency containers first.

The sharded qualification environment runs two three-member shard replica sets, a three-member config server replica set,
two `mongos` routers, and Elasticsearch:

```bash
make start-mongodb-sharded-qualification
export MONGODB_SEEDS=127.0.0.1:27017,127.0.0.1:27018
export MONGODB_REPLICA_SET=""

go test -tags 'test_dep integration' -count=1 ./common/persistence/mongodb
go test -tags 'test_dep integration' -count=1 ./common/persistence/tests -run '^TestMongoDB'
go test -tags 'test_dep integration' -count=1 ./tests \
  -run '^TestMongoDBPersistenceQualificationSuite$' \
  -persistenceType nosql \
  -persistenceDriver mongodb

make stop-mongodb-sharded-qualification
```

The routers use ports 27017-27018, config servers use 27101-27103, shard members use 27201-27203 and 27301-27303,
and Elasticsearch uses 9200. An explicitly empty `MONGODB_REPLICA_SET` selects `mongos` discovery instead of the replica-set default.
