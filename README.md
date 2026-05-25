# IoT Weather Station Monitoring Platform

A production-grade, distributed real-time weather monitoring system built with Java, Apache Kafka, and Kubernetes. Ingests data from 10 simulated IoT weather stations, processes streams in real-time, archives to Parquet, and visualizes via Kibana.

---

## Architecture

```
 DATA ACQUISITION          PROCESSING & ARCHIVING          INDEXING
 ─────────────────         ──────────────────────          ────────
 Station 1 ──┐
 Station 2 ──┤             ┌─────────────────────┐         ┌──────────────┐
 Station 3 ──┼──► Kafka ──►│  Central Station    │────────►│  BitCask LSM │
    ...      │             │  (Kafka Consumer)   │         │  latest/stn  │
 Station 10 ─┘             │                     │         └──────────────┘
                           │  Kafka Streams      │         ┌──────────────┐
                           │  Rain Detector ────►│ rain-   │  Parquet     │
                           │  (humidity > 70%)   │ alerts  │  /year/month │
                           └─────────────────────┘  topic  │  /day/stn    │
                                                           └──────┬───────┘
                                                                  │
                                                           ┌──────▼───────┐
                                                           │  Elastic     │
                                                           │  Search +    │
                                                           │  Kibana      │
                                                           └──────────────┘
```

---

## Components

### Weather Station Simulator
- Each station emits one JSON reading per second
- Battery status distribution: **30% low / 40% medium / 30% high**
- **10% of messages randomly dropped** to simulate network unreliability
- Sequence numbers (`s_no`) increment even on drops — enables gap-based drop detection at the consumer
- Configurable via environment variable: `KAFKA_BOOTSTRAP_SERVERS`

**Message schema:**
```json
{
  "station_id": 1,
  "s_no": 42,
  "battery_status": "medium",
  "status_timestamp": 1681521224,
  "weather": {
    "humidity": 71,
    "temperature": 85,
    "wind_speed": 23
  }
}
```

### Kafka Cluster
- Topic `weather-readings` — 10 partitions, one per station (keyed by `station_id`)
- Topic `rain-alerts` — output of the Streams rain detector
- Zookeeper-backed, Bitnami images

### Kafka Streams Rain Processor
- Detects humidity > 70% and forwards to `rain-alerts` topic
- Built with Kafka Streams DSL

### Central Base Station
- Consumes from `weather-readings`
- Writes latest reading per station to **BitCask** (custom implementation)
- Archives all readings to **Parquet files**, partitioned by `year/month/day/station_id`
- Batch Parquet writes at 10,000 records for I/O efficiency

### BitCask Storage Engine (custom implementation)
- Append-only segment files + in-memory keydir for O(1) reads and writes
- Hint files for fast crash recovery
- Scheduled compaction to merge stale segments
- HTTP API consumed by `bitcask_client.sh`

### Elasticsearch + Kibana
- Indexes all Parquet data for historical analysis
- Kibana dashboards confirming:
  - Battery status distribution (~30/40/30)
  - Dropped message rate per station (~10%)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Build | Maven 3.9 (multi-module) |
| Message Queue | Apache Kafka 3.7 + Zookeeper |
| Stream Processing | Kafka Streams DSL |
| Key-Value Store | BitCask Riak (custom Java impl) |
| Archival Format | Apache Parquet 1.14 |
| Search & Analytics | Elasticsearch 8.x + Kibana |
| Containerization | Docker + Docker Compose |
| Orchestration | Kubernetes 1.27+ |
| Profiling | Java Flight Recorder (JFR) |

---

## Getting Started

### Prerequisites
- Java 17+
- Maven 3.9+
- Docker + Docker Compose

### 1. Build
```bash
mvn clean package
```

### 2. Start infrastructure (Kafka + Zookeeper)
```bash
docker compose up -d

# Wait for Kafka to be healthy (~20 seconds)
docker compose ps
```

### 3. Run a weather station locally
```bash
# stdout mode (no Kafka)
java -jar weather-station/target/weather-station-1.0.0.jar 1

# Kafka mode
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
  java -jar weather-station/target/weather-station-1.0.0.jar 1
```

### 4. Verify messages in Kafka
```bash
docker exec weather-kafka \
  kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic weather-readings \
  --from-beginning
```

---

## Project Structure

```
.
├── pom.xml                          # Parent POM — dependency version management
├── docker-compose.yml               # Local dev: Kafka, Zookeeper, ES, Kibana
├── weather-station/                 # IoT station simulator
│   └── src/main/java/com/weather/station/
│       ├── WeatherStation.java      # Main simulator loop
│       ├── model/
│       │   ├── WeatherReading.java  # Full message envelope
│       │   └── WeatherData.java     # Nested weather measurements
│       └── producer/
│           └── WeatherKafkaProducer.java
├── central-station/                 # Stream processor + storage engine
│   └── src/main/java/com/weather/central/
│       ├── CentralStation.java
│       ├── bitcask/                 # Custom BitCask implementation
│       ├── parquet/                 # Parquet batch writer
│       └── streams/                # Kafka Streams rain detector
└── k8s/                            # Kubernetes manifests
```

---

## Kubernetes Deployment

```bash
# Apply all manifests
kubectl apply -f k8s/

# Check pods
kubectl get pods -n weather-platform
```

The K8s setup provisions:
- 10 weather station pods (each with `STATION_ID` env var 1–10)
- 1 central station pod
- Kafka + Zookeeper StatefulSets
- Elasticsearch + Kibana
- PersistentVolumeClaims for Parquet and BitCask storage

---

## BitCask CLI Client

```bash
# View all keys → timestamped CSV
./bitcask_client.sh --view-all

# View a single key
./bitcask_client.sh --view --key=3

# Performance test: 100 concurrent reader threads
./bitcask_client.sh --perf --clients=100
```

---

## JFR Profiling

Run the central station with JFR enabled:
```bash
java -XX:StartFlightRecording=duration=60s,filename=central.jfr \
     -jar central-station/target/central-station-1.0.0.jar
```

Analyze with JDK Mission Control or the `jfr` CLI tool.
