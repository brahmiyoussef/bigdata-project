#!/bin/bash

# MongoDB container and database details
MONGO_CONTAINER="mongo"  # MongoDB container name
DATABASE_NAME="market_screener"  # MongoDB database to back up
BACKUP_DIR="/data/backup"  # Directory within mongo-db volume
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MINIO_VOLUME_PATH="/mnt/c/Users/HP/Desktop/big_data/project_name/data/miniodata"  # Path to the MinIO volume on the host

# MinIO bucket and client alias
MINIO_ALIAS="myminio"
MINIO_BUCKET="mongodb-buckup"  # MinIO bucket for storing backups
MINIO_BACKUP_DIR="/data"  # Root directory of minio-data volume
HOST_BACKUP_DIR="/mnt/c/Users/HP/Desktop/big_data/project_name/data"  # Directory on the host where we temporarily store backup before moving to MinIO volume

# Collections to back up
COLLECTIONS=("topic_AAPL" "topic_AMZN" "topic_GOOG" "topic_META" "topic_MSFT" "topic_NVDA" "topic_TSLA" "market_screen")

for COLLECTION in "${COLLECTIONS[@]}"; do
    BACKUP_NAME="${DATABASE_NAME}_${COLLECTION}_backup_$TIMESTAMP.gz"
    BACKUP_PATH="$BACKUP_DIR/$BACKUP_NAME"

    # Step 1: Run mongodump in MongoDB container for each collection
    echo "Creating MongoDB backup for collection $COLLECTION..."
    docker exec $MONGO_CONTAINER bash -c "mkdir -p $BACKUP_DIR && mongodump --db $DATABASE_NAME --collection $COLLECTION --archive=$BACKUP_PATH --gzip"

    # Step 2: Copy backup file from mongo-db volume to minio-data volume using a temporary container
    echo "Copying backup file from mongo-db volume to minio-data volume..."
    docker cp $MONGO_CONTAINER:$BACKUP_DIR/$BACKUP_NAME $HOST_BACKUP_DIR

    # Step 3: Move the backup from host directory to the MinIO volume
    echo "Moving backup to MinIO volume directory..."
    mv $HOST_BACKUP_DIR/$BACKUP_NAME $MINIO_VOLUME_PATH

    # Step 4: Upload backup to MinIO
    echo "Uploading backup to MinIO..."
    docker exec minio mc cp "$MINIO_BACKUP_DIR/$BACKUP_NAME" "$MINIO_ALIAS/$MINIO_BUCKET/"

    # Step 5: Clean up backup file from both volumes
    echo "Cleaning up backup files..."
    docker exec $MONGO_CONTAINER rm -f $BACKUP_PATH
    docker exec minio rm -f "$MINIO_BACKUP_DIR/$BACKUP_NAME"
    rm -f $HOST_BACKUP_DIR/$BACKUP_NAME
done

echo "All backups completed and stored in MinIO bucket '$MINIO_BUCKET'."
