#!/bin/bash

# Define the topics to create
TOPICS=(
  "connect-offsets"
  "connect-configs"
  "connect-status"
)

# Define the configurations for each topic
CONFIGS=(
  "cleanup.policy=compact"
)

# Kafka bootstrap server
BOOTSTRAP_SERVER=kafka:9092

# Function to delete and recreate a topic
create_topic() {
  local topic=$1
  echo "Deleting topic $topic if it exists"
  kafka-topics.sh --delete --topic "$topic" --bootstrap-server "$BOOTSTRAP_SERVER" --if-exists
  echo "Creating topic $topic with ${CONFIGS[@]}"
  kafka-topics.sh --create --topic "$topic" --partitions 1 --replication-factor 1 --config "${CONFIGS[@]}" --bootstrap-server "$BOOTSTRAP_SERVER"
}

# Create the topics
for topic in "${TOPICS[@]}"; do
  create_topic "$topic"
done
