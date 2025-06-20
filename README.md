# Real-Time Anomaly Detection with Apache Storm

This repository contains a sample topology for processing IoT sensor data and detecting anomalies in real time. It demonstrates how to use **Apache Storm** together with Kafka, Redis and TimescaleDB to build a scalable alerting pipeline.

## Overview

The topology ingests JSON messages from Kafka topics, cleans the data using several bolts and emits alerts when sensor values exceed configurable thresholds. Cleaned data and alerts are stored in TimescaleDB while notifications are sent via e-mail.

## Architecture

- **KafkaSpout** – consumes sensor readings from Kafka.
- **InputParsingBolt** – parses JSON and normalizes tuples.
- **BadTimestampBolt** – adjusts inconsistent timestamps.
- **DataOutlierBolt** – filters measurements outside realistic bounds.
- **FalseSpikeBolt** – detects sudden spikes using a moving average.
- **ValueBlockedBolt** – identifies sensors stuck on the same value.
- **ThresholdBolt** – compares values with limits stored in Redis and emits events when thresholds are crossed.
- **TimescaleDBBolt** – persists cleaned measurements to TimescaleDB.
- **AlertBolt** – stores alerts in the database and sends email notifications.

The main topology is implemented in `SensorTopology.java`.

## Prerequisites

- Java 21
- Maven 3.x
- Kafka broker with the required topics
- Redis instance for configuration and e-mail list
- TimescaleDB or PostgreSQL instance
- Apache Storm cluster (local or distributed)

## Building

```bash
mvn clean package
```

This produces a shaded jar in the `target/` directory.

## Running

Submit the topology to your Storm cluster:

```bash
storm jar target/LICENTA-1.0-SNAPSHOT.jar \
    org.project.SensorTopology <kafkaBroker> <redisHost:port> \
    <timescaleUrl> <dbUser> <dbPass> <workerCount>
```

Replace the placeholders with your environment configuration.

## Directory Layout

```
Apache-Storm-real-time-anomaly-detection/
├── src/
│   ├── main/java/       # topology, bolts and helpers
│   └── test/java/       # unit tests
├── pom.xml              # Maven build file
└── README.md            # this file
```

## Testing

Run unit tests with Maven:

```bash
mvn test
```

## License

This project is provided for demonstration purposes.
