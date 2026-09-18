#!/usr/bin/env bash
set -euo pipefail

readonly username="${MONGO_INITDB_ROOT_USERNAME}"
readonly password="${MONGO_INITDB_ROOT_PASSWORD}"
readonly keyfile="/data/configdb/mongo-keyfile"

printf '%s' "${MONGO_REPLICA_SET_KEY}" >"${keyfile}"
chmod 400 "${keyfile}"

mongod_pids=()

start_mongod() {
  local replica_set="$1"
  local port="$2"
  local role="$3"
  local dbpath="/data/${replica_set}-${port}"
  mkdir -p "${dbpath}"

  role_args=()
  if [[ "${role}" == "config" ]]; then
    role_args=(--configsvr)
  else
    role_args=(--shardsvr)
  fi

  mongod \
    "${role_args[@]}" \
    --replSet "${replica_set}" \
    --port "${port}" \
    --bind_ip_all \
    --dbpath "${dbpath}" \
    --keyFile "${keyfile}" \
    --wiredTigerCacheSizeGB 0.25 \
    --setParameter enableTestCommands=1 \
    --pidfilepath "${dbpath}/mongod.pid" \
    --logpath "${dbpath}/mongod.log" \
    --fork
  mongod_pids+=("$(cat "${dbpath}/mongod.pid")")
}

start_mongos() {
  local port="$1"
  local dbpath="/data/mongos-${port}"
  mkdir -p "${dbpath}"
  mongos \
    --configdb config-rs/localhost:27101,localhost:27102,localhost:27103 \
    --port "${port}" \
    --bind_ip_all \
    --keyFile "${keyfile}" \
    --setParameter enableTestCommands=1 \
    --pidfilepath "${dbpath}/mongos.pid" \
    --logpath "${dbpath}/mongos.log" \
    --fork
}

wait_for_port() {
  local port="$1"
  until mongosh --quiet --port "${port}" --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1; do
    sleep 1
  done
}

init_replica_set() {
  local replica_set="$1"
  local first_port="$2"
  local second_port="$3"
  local third_port="$4"

  wait_for_port "${first_port}"
  mongosh --quiet --port "${first_port}" --eval "
    try {
      rs.status();
    } catch (error) {
      if (error.codeName !== 'NotYetInitialized') throw error;
      rs.initiate({
        _id: '${replica_set}',
        members: [
          { _id: 0, host: 'localhost:${first_port}', priority: 3 },
          { _id: 1, host: 'localhost:${second_port}', priority: 2 },
          { _id: 2, host: 'localhost:${third_port}', priority: 1 }
        ]
      });
    }
  "
  until mongosh --quiet --port "${first_port}" --eval 'if (!db.adminCommand({ hello: 1 }).isWritablePrimary) quit(2)' >/dev/null 2>&1; do
    sleep 1
  done
  mongosh --quiet --port "${first_port}" --eval "
    const admin = db.getSiblingDB('admin');
    admin.createUser({
      user: '${username}',
      pwd: '${password}',
      roles: [{ role: 'root', db: 'admin' }]
    });
  "
}

start_mongod config-rs 27101 config
start_mongod config-rs 27102 config
start_mongod config-rs 27103 config
start_mongod shard-rs-0 27201 shard
start_mongod shard-rs-0 27202 shard
start_mongod shard-rs-0 27203 shard
start_mongod shard-rs-1 27301 shard
start_mongod shard-rs-1 27302 shard
start_mongod shard-rs-1 27303 shard

init_replica_set config-rs 27101 27102 27103
init_replica_set shard-rs-0 27201 27202 27203
init_replica_set shard-rs-1 27301 27302 27303

for port in 27017 27018; do
  start_mongos "${port}"
done

readonly mongo_uri="mongodb://${username}:${password}@localhost:27017/admin"
until mongosh --quiet "${mongo_uri}" --eval 'if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") quit(2)' >/dev/null 2>&1; do
  sleep 1
done

mongosh --quiet "${mongo_uri}" --eval "
  const existing = new Set(db.adminCommand({ listShards: 1 }).shards.map((shard) => shard._id));
  if (!existing.has('shard-rs-0')) {
    const result = db.adminCommand({ addShard: 'shard-rs-0/localhost:27201,localhost:27202,localhost:27203' });
    if (!result.ok) throw new Error(JSON.stringify(result));
  }
  if (!existing.has('shard-rs-1')) {
    const result = db.adminCommand({ addShard: 'shard-rs-1/localhost:27301,localhost:27302,localhost:27303' });
    if (!result.ok) throw new Error(JSON.stringify(result));
  }
"

shutdown() {
  for port in 27017 27018; do
    pidfile="/data/mongos-${port}/mongos.pid"
    if [[ -f "${pidfile}" ]]; then
      kill "$(cat "${pidfile}")" 2>/dev/null || true
    fi
  done
  for pid in "${mongod_pids[@]}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait || true
}
trap shutdown TERM INT EXIT

while true; do
  for pid in "${mongod_pids[@]}"; do
    kill -0 "${pid}"
  done
  for port in 27017 27018; do
    pidfile="/data/mongos-${port}/mongos.pid"
    if [[ ! -f "${pidfile}" ]] || ! kill -0 "$(cat "${pidfile}")" 2>/dev/null; then
      start_mongos "${port}"
    fi
  done
  sleep 1
done
