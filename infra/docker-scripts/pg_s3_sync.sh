#!/bin/bash
set -e

# Configuration
S3_ENDPOINT=${MLFLOW_S3_ENDPOINT_URL}
S3_BUCKET=${MLFLOW_S3_BUCKET}
BACKUP_PREFIX="db-backups"
PG_HOST="postgres"
PG_USER=${POSTGRES_USER:-airflow}
PG_DB=${POSTGRES_DB:-airflow}
export PGPASSWORD=${POSTGRES_PASSWORD}

# Configure AWS CLI
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
aws configure set default.region "${AWS_DEFAULT_REGION:-us-east-1}"

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

restore_backup() {
    echo "Checking for existing backups in s3://$S3_BUCKET/$BACKUP_PREFIX..."
    
    LATEST_BACKUP=$(aws $AWS_ARGS s3 ls "s3://$S3_BUCKET/$BACKUP_PREFIX/" | sort | tail -n 1 | awk '{print $4}')
    
    if [ -z "$LATEST_BACKUP" ]; then
        echo "No backups found. Starting fresh."
        psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "CREATE DATABASE mlflow_db;" || true
        touch /tmp/healthy
        return 0
    fi
    
    echo "Found backup: $LATEST_BACKUP"
    
    # Check if DB has tables
    TABLE_COUNT=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';")
    
    if [ "$TABLE_COUNT" -gt "0" ] && [ "$FORCE_RESTORE" != "true" ]; then
        echo "Database is not empty ($TABLE_COUNT tables). Skipping restore."
        # Ensure mlflow_db exists if we are skipping restore
        psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "CREATE DATABASE mlflow_db;" || true
        touch /tmp/healthy
        return 0
    fi

    echo "Restoring form $LATEST_BACKUP..."
    aws $AWS_ARGS s3 cp "s3://$S3_BUCKET/$BACKUP_PREFIX/$LATEST_BACKUP" /tmp/restore.dump
    
    pg_restore -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" --clean --if-exists --no-owner --role="$PG_USER" /tmp/restore.dump || true
    
    # Ensure mlflow_db exists after restore (if dump didn't have it or we restored to 'airflow' DB)
    psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "CREATE DATABASE mlflow_db;" || true

    echo "Restore completed."
    rm /tmp/restore.dump
    touch /tmp/healthy
}

backup_loop() {
    while true; do
        current_hour=$(date +%H)
        INTERVAL=${BACKUP_INTERVAL_SECONDS:-3600}
        
        echo "Sleeping for $INTERVAL seconds..."
        sleep $INTERVAL
        
        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        BACKUP_FILE="${PG_DB}_${TIMESTAMP}.dump"
        
        echo "Creating backup: $BACKUP_FILE"
        pg_dump -h "$PG_HOST" -U "$PG_USER" -F c -b -v -f "/tmp/$BACKUP_FILE" "$PG_DB"
        
        echo "Uploading to S3..."
        aws $AWS_ARGS s3 cp "/tmp/$BACKUP_FILE" "s3://$S3_BUCKET/$BACKUP_PREFIX/$BACKUP_FILE"
        
        rm "/tmp/$BACKUP_FILE"
        echo "Backup done."
    done
}

wait_for_postgres

if [ "$1" = "restore_and_loop" ]; then
    restore_backup
    backup_loop
else
    exec "$@"
fi
