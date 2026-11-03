#!/bin/sh
# Restores the database from R2 if there is a replica and no local file, then runs
# Rainwise under Litestream so every change is replicated. Litestream forwards
# SIGTERM to the app and waits for it to exit.
set -eu

DB=/data/rainwise.db

# -if-replica-exists: a first deploy has nothing to restore and must still start.
# A real restore error (bad credentials, say) fails here, and the container
# exits rather than starting with an empty database.
litestream restore -config /etc/litestream.yml -if-db-not-exists -if-replica-exists "$DB"

exec litestream replicate -config /etc/litestream.yml \
  -exec "rainwise serve --config /etc/rainwise/config.yaml"
