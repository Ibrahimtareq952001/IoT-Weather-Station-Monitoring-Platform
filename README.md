<img width="100%" src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,20,6&height=180&section=header&text=IoT%20Weather%20Station&fontSize=40&fontColor=fff&animation=twinkling&fontAlignY=38&desc=Distributed%20Real-Time%20Monitoring%20%E2%80%94%20Kafka%20%7C%20Kubernetes%20%7C%20ElasticSearch&descAlignY=58&descSize=16&descColor=cbd5e1"/>

<div align="center">

![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=openjdk&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?style=flat-square&logo=elasticsearch&logoColor=white)
![Kibana](https://img.shields.io/badge/Kibana-E8488B?style=flat-square&logo=kibana&logoColor=white)

</div>

---

## Overview

A production-grade distributed IoT monitoring platform that ingests real-time weather data from **10 simulated weather stations**, processes streams using **Kafka Streams** for anomaly detection, persists data via a **custom BitCask LSM store** and **Parquet archival**, indexes into **Elasticsearch**, and visualizes through **Kibana** — all orchestrated on **Kubernetes**.

Implements **6 Enterprise Integration Patterns** (EIP) including message routing, content-based routing, and competing consumers.

---

## Architecture

```
 ACQUISITION              PROCESSING & STORAGE              VISUALIZATION
 ───────────              ────────────────────              ─────────────

 Station 1 ──┐
 Station 2 ──┤            ┌──────────────────┐
 Station 3 ──┤            │  Central Station │──► BitCask LSM   (latest)
    ...      ├──► Kafka ──│  (Kafka Consumer)│──► Parquet       (archive)
 Station 10 ─┘            │                  │
                          │  Kafka Streams   │──► rain-alerts topic
                          │  Rain Detector   │         │
                          │  (humidity >70%) │         ▼
                          └──────────────────┘   ElasticSearch
                                                       │
                                                       ▼
                                                    Kibana
                                                  Dashboards
```

---

## Key Features

- **10 concurrent weather stations** — each runs as an independent process generating temperature, humidity, pressure, and wind data
- **Kafka ingestion** — all station data streams into a single Kafka topic with station-keyed partitioning
- **Kafka Streams anomaly detection** — real-time rain alert detection (humidity > 70%) with output to a separate alerts topic
- **Dual persistence** — custom BitCask LSM store for latest readings + Parquet columnar archival partitioned by year/month/day/station
- **ElasticSearch + Kibana** — full-text search and time-series dashboards over all ingested data
- **Kubernetes orchestration** — all services deployed as K8s pods with health checks and auto-restart

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Weather Stations | Java (multi-threaded producers) |
| Message Broker | Apache Kafka |
| Stream Processing | Kafka Streams |
| Storage Engine | Custom BitCask LSM (Java) |
| Archival Format | Apache Parquet |
| Search & Index | Elasticsearch |
| Visualization | Kibana |
| Orchestration | Kubernetes, Docker |

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Java 17+
- Maven

### Run with Docker Compose

```bash
git clone https://github.com/Ibrahimtareq952001/IoT-Weather-Station-Monitoring-Platform.git
cd IoT-Weather-Station-Monitoring-Platform

# Start Kafka, Elasticsearch, Kibana
docker-compose up -d

# Build and run the central station
mvn clean package -f central-station/pom.xml
java -jar central-station/target/central-station.jar

# In separate terminals, start weather stations
mvn clean package -f weather-station/pom.xml
java -jar weather-station/target/weather-station.jar --id=1
# ... repeat for stations 2-10
```

### Access Kibana
Open [http://localhost:5601](http://localhost:5601) to view dashboards.

---

<div align="center">

*Distributed Systems — Alexandria University 2025*

[![Resume](https://img.shields.io/badge/View_Resume-PDF-008080?style=flat-square&logo=latex&logoColor=white)](https://github.com/Ibrahimtareq952001/Resume/blob/main/resume.pdf)
[![Portfolio](https://img.shields.io/badge/GitHub-Ibrahimtareq952001-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/Ibrahimtareq952001)

</div>

<img width="100%" src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,20,6&height=100&section=footer"/>
