#!/usr/bin/env bash
set -euo pipefail

readonly replica_set="${MONGO_REPLICA_SET:-rs0}"
readonly keyfile="/data/configdb/mongo-keyfile"

mkdir -p /data/db1 /data/db2 /data/db3 "$(dirname "${keyfile}")"
printf '%s' "${MONGO_REPLICA_SET_KEY}" >"${keyfile}"
chmod 400 "${keyfile}"

pids=()
for member in 1 2 3; do
  port=$((27016 + member))
  mongod \
    --replSet "${replica_set}" \
    --port "${port}" \
    --bind_ip_all \
    --dbpath "/data/db${member}" \
    --keyFile "${keyfile}" \
    --setParameter enableTestCommands=1 \
    --pidfilepath "/data/db${member}/mongod.pid" \
    --logpath "/data/db${member}/mongod.log" \
    --fork
  pids+=("$(cat "/data/db${member}/mongod.pid")")
done

until mongosh --quiet --port 27017 --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1; do
  sleep 1
done

auth_args=()
if mongosh --quiet --port 27017 \
  --username "${MONGO_INITDB_ROOT_USERNAME}" \
  --password "${MONGO_INITDB_ROOT_PASSWORD}" \
  --authenticationDatabase admin \
  --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1; then
  auth_args=(
    --username "${MONGO_INITDB_ROOT_USERNAME}"
    --password "${MONGO_INITDB_ROOT_PASSWORD}"
    --authenticationDatabase admin
  )
fi

mongosh --quiet --port 27017 "${auth_args[@]}" --eval "
  try {
    rs.status();
  } catch (error) {
    if (error.codeName !== 'NotYetInitialized') throw error;
    rs.initiate({
      _id: '${replica_set}',
      members: [
        { _id: 0, host: 'localhost:27017', priority: 3 },
        { _id: 1, host: 'localhost:27018', priority: 2 },
        { _id: 2, host: 'localhost:27019', priority: 1 }
      ]
    });
  }
"

until mongosh --quiet --port 27017 "${auth_args[@]}" --eval 'if (!db.adminCommand({ hello: 1 }).isWritablePrimary) quit(2)' >/dev/null 2>&1; do
  sleep 1
done

if ((${#auth_args[@]} == 0)); then
  mongosh --quiet --port 27017 --eval "
    const admin = db.getSiblingDB('admin');
    admin.createUser({
      user: '${MONGO_INITDB_ROOT_USERNAME}',
      pwd: '${MONGO_INITDB_ROOT_PASSWORD}',
      roles: [{ role: 'root', db: 'admin' }]
    });
  "
fi

shutdown() {
  for pid in "${pids[@]}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait || true
}
trap shutdown TERM INT EXIT

while true; do
  for pid in "${pids[@]}"; do
    kill -0 "${pid}"
  done
  sleep 1
done
