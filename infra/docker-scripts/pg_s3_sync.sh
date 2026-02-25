#!/bin/bash
set -euo pipefail

# Configuration
S3_ENDPOINT=${MLFLOW_S3_ENDPOINT_URL:-}
S3_BUCKET=${MLFLOW_S3_BUCKET:-}
BACKUP_PREFIX="db-backups"
PG_HOST="postgres"
PG_USER=${POSTGRES_USER:-airflow}
POSTGRES_DB=${POSTGRES_DB:-airflow}
# List of DBs to backup/restore, comma separated (default: POSTGRES_DB and mlflow_db)
BACKUP_DBS=${BACKUP_DBS:-"${POSTGRES_DB},mlflow_db"}
export PGPASSWORD=${POSTGRES_PASSWORD:-}

# Configure AWS CLI
if [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
    aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
    aws configure set default.region "${AWS_DEFAULT_REGION:-us-east-1}"
fi

AWS_ARGS=""
if [ -n "$S3_ENDPOINT" ]; then
    AWS_ARGS="--endpoint-url $S3_ENDPOINT"
fi

touch /tmp/unhealthy # Start unhealthy

wait_for_postgres() {
    echo "Waiting for Postgres at $PG_HOST..."
    until pg_isready -h "$PG_HOST" -U "$PG_USER"; do
        sleep 2
    done
}

ensure_db_exists() {
    local dbname="$1"
    echo "Ensuring database $dbname exists..."
    if ! psql -h "$PG_HOST" -U "$PG_USER" -lqt | cut -d \| -f 1 | grep -qw "$dbname"; then
        echo "Creating database $dbname"
        psql -h "$PG_HOST" -U "$PG_USER" -c "CREATE DATABASE \"$dbname\";" || true
    else
        echo "Database $dbname already exists"
    fi
}

restore_backup() {
    echo "Checking for existing backups in s3://$S3_BUCKET/$BACKUP_PREFIX..."

    IFS=',' read -r -a dbs <<< "$BACKUP_DBS"
    for db in "${dbs[@]}"; do
        db=$(echo "$db" | xargs)
        echo "Looking for latest backup for DB: $db"
        LATEST_BACKUP=$(aws $AWS_ARGS s3 ls "s3://$S3_BUCKET/$BACKUP_PREFIX/" | awk '{print $4}' | grep "^${db}_" | sort | tail -n 1 || true)

        if [ -z "$LATEST_BACKUP" ]; then
            echo "No backup found for $db. Will ensure DB exists."
            ensure_db_exists "$db"
            continue
        fi

        echo "Found backup for $db: $LATEST_BACKUP"
        aws $AWS_ARGS s3 cp "s3://$S3_BUCKET/$BACKUP_PREFIX/$LATEST_BACKUP" "/tmp/${LATEST_BACKUP}"

        echo "Restoring $db from /tmp/${LATEST_BACKUP}"
        # try pg_restore (custom format), fallback to psql for plain SQL
        if file "/tmp/${LATEST_BACKUP}" | grep -q "gzip\|compressed"; then
            echo "Compressed or unknown format, attempting pg_restore"
            pg_restore -h "$PG_HOST" -U "$PG_USER" -d "$db" --clean --if-exists --no-owner --role="$PG_USER" "/tmp/${LATEST_BACKUP}" || true
        else
            # Assume custom format or plain; try pg_restore first, then psql
            if ! pg_restore -h "$PG_HOST" -U "$PG_USER" -d "$db" --clean --if-exists --no-owner --role="$PG_USER" "/tmp/${LATEST_BACKUP}"; then
                echo "pg_restore failed, trying psql (plain SQL)"
                psql -h "$PG_HOST" -U "$PG_USER" -d "$db" -f "/tmp/${LATEST_BACKUP}" || true
            fi
        fi

        rm -f "/tmp/${LATEST_BACKUP}"
        echo "Restore for $db completed."
    done

    touch /tmp/healthy
}

backup_loop() {
    IFS=',' read -r -a dbs <<< "$BACKUP_DBS"
    while true; do
        INTERVAL=${BACKUP_INTERVAL_SECONDS:-3600}
        echo "Sleeping for $INTERVAL seconds..."
        sleep $INTERVAL

        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        for db in "${dbs[@]}"; do
            db=$(echo "$db" | xargs)
            BACKUP_FILE="${db}_${TIMESTAMP}.dump"
            echo "Creating backup for DB $db: $BACKUP_FILE"
            pg_dump -h "$PG_HOST" -U "$PG_USER" -F c -b -v -f "/tmp/$BACKUP_FILE" "$db" || true
            echo "Uploading $BACKUP_FILE to s3://$S3_BUCKET/$BACKUP_PREFIX/"
            aws $AWS_ARGS s3 cp "/tmp/$BACKUP_FILE" "s3://$S3_BUCKET/$BACKUP_PREFIX/$BACKUP_FILE" || true
            rm -f "/tmp/$BACKUP_FILE"
        done

        echo "Backup loop iteration completed."
    done
}

wait_for_postgres

if [ "${1:-}" = "restore_and_loop" ]; then
    restore_backup
    backup_loop
else
    exec "$@"
fi
