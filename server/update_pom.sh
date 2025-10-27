#!/bin/bash

# Path to pom.xml
POM_FILE="debezium-server/pom.xml"

# Backup original pom.xml
cp "$POM_FILE" "${POM_FILE}.bak"

# Replace debezium-server-http with debezium-server-console
sed -i 's|<module>debezium-server-http</module>|<module>debezium-server-console</module>|' "$POM_FILE"
sed -i 's|<module>debezium-server-kinesis</module>|<module>debezium-server-exasol</module>|' "$POM_FILE"

# Remove the other listed modules
# sed -i '/<module>debezium-server-kinesis<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-pubsub<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-pulsar<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-milvus<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-qdrant<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-eventhubs<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-redis<\/module>/d' "$POM_FILE"
# sed -i '/<module>debezium-server-dist<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-kafka<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-pravega<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-nats-streaming<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-nats-jetstream<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-infinispan<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-rabbitmq<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-rocketmq<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-sqs<\/module>/d' "$POM_FILE"
sed -i '/<module>debezium-server-instructlab<\/module>/d' "$POM_FILE"

echo "pom.xml updated. Backup saved as pom.xml.bak"



