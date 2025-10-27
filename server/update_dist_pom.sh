#!/bin/bash

POM_FILE="debezium-server/debezium-server-dist/pom.xml"
BACKUP_FILE="${POM_FILE}.bak"

# Backup original pom.xml
cp "$POM_FILE" "$BACKUP_FILE"

# Replace debezium-server-http with debezium-server-console
sed -i 's|<artifactId>debezium-server-http</artifactId>|<artifactId>debezium-server-console</artifactId>|g' "$POM_FILE"
sed -i 's|<artifactId>debezium-server-kinesis</artifactId>|<artifactId>debezium-server-exasol</artifactId>|g' "$POM_FILE"

# List of artifactIds to remove
ARTIFACTS=(
    # "debezium-server-kinesis"
    "debezium-server-sqs"
    "debezium-server-pubsub"
    "debezium-server-pulsar"
    "debezium-server-eventhubs"
    "debezium-server-redis"
    "debezium-server-kafka"
    "debezium-server-pravega"
    "debezium-server-nats-streaming"
    "debezium-server-nats-jetstream"
    "debezium-server-infinispan"
    "debezium-server-rabbitmq"
    "debezium-server-rocketmq"
    "debezium-server-milvus"
    "debezium-server-qdrant"
    "debezium-server-instructlab"
)

# Convert array to regex pattern
PATTERN=$(printf "|%s" "${ARTIFACTS[@]}")
PATTERN=${PATTERN:1}  # remove leading |

# Remove dependency blocks containing listed artifactIds
awk -v pattern="$PATTERN" '
    BEGIN {skip=0}
    /<dependency>/ {buffer=$0"\n"; skip=0; next}
    /<\/dependency>/ {
        buffer=buffer $0 "\n";
        if (buffer ~ pattern) {
            skip=1
        }
        if (!skip) {
            printf "%s", buffer
        }
        buffer=""
        next
    }
    { if (buffer) buffer=buffer $0 "\n"; else print }
' "$POM_FILE" > "${POM_FILE}.tmp"

mv "${POM_FILE}.tmp" "$POM_FILE"

echo "pom.xml updated. Backup saved as $BACKUP_FILE"
