# Apache Storm Real-Time Anomaly Detection

This project demonstrates real-time anomaly detection using **Apache Storm**, a distributed real-time computation system. The system is designed to ingest, process, and analyze streaming data to identify anomalies on the fly.

## Features

- Stream processing with Apache Storm
- Real-time anomaly detection logic
- Scalable and distributed architecture
- Easy integration with real-time data sources

## Technologies Used

- Apache Storm
- Java
- Maven
- Kafka (optional for real-world deployment)
- Log-based or synthetic data sources

## Project Structure

```
Apache-Storm-real-time-anomaly-detection/
├── src/                     # Java source code for topology and bolt logic
│   └── main/java/
├── pom.xml                  # Maven build file
├── README.md                # Project documentation
└── resources/               # Configurations or sample data (if any)
```

## Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/RaresBran/Apache-Storm-real-time-anomaly-detection.git
   cd Apache-Storm-real-time-anomaly-detection
   ```

2. **Build the project**
   Make sure Maven is installed, then run:
   ```bash
   mvn clean package
   ```

3. **Deploy the topology**
   Submit the Storm topology to your local or remote cluster:
   ```bash
   storm jar target/your-jar-name.jar your.package.TopologyClass
   ```

4. **Feed data to the system**
   Connect a data source or simulate input to start streaming.

## Requirements

- Java 8+
- Apache Storm
- Maven
