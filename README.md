<div align="center">
  <img src="logo.png" alt="IDFM GTFS-RT Bridge Logo" width="400"/>
</div>

# IDFM GTFS-RT Bridge

A Spring Boot application that bridges IDFM (Île-de-France Mobilités) real-time transit data to GTFS-Realtime.

## 📋 Overview

This service fetches real-time transit information from the IDFM network and converts it into standardized GTFS-Realtime Protocol Buffer files. It provides REST endpoints to access:

- **GTFS-RT Alerts**: Service disruptions, delays, and transit alerts
- **GTFS-RT Trip Updates**: Real-time vehicle positions, estimated arrival/departure times, and schedule adherence

## 🌐 Free Online Usage

If you want to **directly get feeds messages online**, you can use my **own HTTP links containing the GTFS-RT feeds** !

#### GTFS-RT Trip Updates : http://gtfsidfm.clarifygdps.com/gtfs-rt-trips-idfm

#### GTFS-RT Alerts : http://gtfsidfm.clarifygdps.com/gtfs-rt-alerts-idfm

> **You can also view a preview of the IDFM GTFS-RT feed using my own [MOTIS](https://github.com/motis-project/motis) instance here:** http://motis.clarifygdps.com/

## ✨ Features

- 🚆 **Real-time Transit Data**: Automatic fetching and processing of IDFM transit data
- 📡 **Multiple Format Support**: GTFS-Realtime (Protocol Buffers) and SIRI-Lite (JSON)
- 🔄 **Scheduled Updates**: Periodic data synchronization with configurable intervals
- 🐋 **Docker Support**: Easy deployment with Docker and Docker Compose
- 📊 **Trip Matching**: Intelligent matching of real-time data with scheduled GTFS trips
- 🗄️ **SQLite Database**: Local GTFS data storage for fast access
- ⚡ **High Performance**: Optimized for handling large transit networks

## 🛠️ Technology Stack

- **Java 21**: Modern Java runtime
- **Spring Boot**: Application framework with scheduling support
- **GTFS Realtime Bindings**: Protocol Buffer handling
- **OneBusAway GTFS**: GTFS data processing
- **SQLite**: Local database storage
- **Maven**: Build and dependency management
- **Docker**: Containerization

## 📦 Prerequisites

- Java 21 or higher
- Maven 3.6+
- Docker & Docker Compose (for containerized deployment)
- Node.js 22+ (for gtfs-import tool)

## 🚀 Quick Start

### Using Pre-built Docker Image from GitHub Container Registry (Recommended)

1. **Create a `.env` file from the example**

    You need to create a `.env` file based on the provided `.env.example`:

2. **Run the container**
   ```bash
   docker run -d \
     --name gtfs_app \
     --env-file .env \
     -p 8507:8507 \
     ghcr.io/jouca/idfm_gtfs-rt:latest
   ```

> The application will be available at `http://localhost:8507`

### Using Docker Compose (Build from Source)

1. **Clone the repository**
   ```bash
   git clone https://github.com/Jouca/IDFM_GTFS-RT.git
   cd IDFM_GTFS-RT
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Build and run with Docker Compose**
   ```bash
   docker compose up -d
   ```

The application will be available at `http://localhost:8507`

### Manual Installation

1. **Clone and build**
   ```bash
   git clone https://github.com/Jouca/IDFM_GTFS-RT.git
   cd IDFM_GTFS-RT
   mvn clean package
   ```

2. **Run the application**
   ```bash
   java -jar target/idfm_gtfs_rt-0.0.1-SNAPSHOT.jar
   ```

## 🔧 Configuration

Configuration is managed through `application.properties` and environment variables:

```properties
# Application name
spring.application.name=idfm_gtfs_rt

# Server port (default: 8507)
server.port=8507

# Cancellation window for theoretical trips
gtfsrt.cancellation.window.minutes=120
```

### Environment Variables

Create a `.env` file with the following variables:

```env
SPRING_APPLICATION_NAME=idfm_gtfs_rt
SERVER_PORT=8507
```

## 📡 API Endpoints

### GET `/gtfs-rt-alerts-idfm`
Download GTFS-RT alerts feed (Protocol Buffer format)

**Response**: Binary `.pb` file containing service alerts

---

### GET `/gtfs-rt-trips-idfm`
Download GTFS-RT trip updates feed (Protocol Buffer format)

**Response**: Binary `.pb` file containing trip updates

---

### POST `/getEntities`
Retrieve specific trip entities by their IDs

**Parameters**:
- `tripIds` (required): Comma-separated list of trip IDs

**Example**:
```bash
curl -X POST "http://localhost:8080/getEntities?tripIds=trip1,trip2,trip3"
```

**Response**: JSON object mapping trip IDs to their entity data

## 🏗️ Project Structure

```
idfm_gtfs-rt/
├── src/main/java/org/jouca/idfm_gtfs_rt/
│   ├── GTFSRTApplication.java       # Main application entry point
│   ├── controller/
│   │   └── GTFSRTController.java    # REST API endpoints
│   ├── fetchers/
│   │   ├── AlertFetcher.java        # Fetches alert data
│   │   ├── GTFSFetcher.java         # Fetches GTFS static data
│   │   └── SiriLiteFetcher.java     # Fetches SIRI-Lite data
│   ├── finders/
│   │   └── TripFinder.java          # Matches real-time data to trips
│   ├── generator/
│   │   ├── AlertGenerator.java      # Generates GTFS-RT alerts
│   │   └── TripUpdateGenerator.java # Generates GTFS-RT trip updates
│   ├── records/
│   │   └── EstimatedCall.java       # Data models
│   └── services/
│       └── ScheduledTasks.java      # Background data update tasks
├── docker-compose.yml               # Docker Compose configuration
├── Dockerfile                       # Docker image definition
└── pom.xml                         # Maven dependencies
```

## 🔄 How It Works

1. **Data Fetching**: The application periodically fetches GTFS static data and real-time updates from IDFM
2. **Trip Matching**: Real-time data is matched with scheduled trips using the `TripFinder`
3. **Format Conversion**: Data is converted to GTFS-RT Protocol Buffers
4. **File Generation**: Updated feeds are written to `.pb` and `.json` files
5. **API Serving**: REST endpoints serve the latest feed data to clients

## 🐳 Docker Deployment

The application is containerized for easy deployment:

- **Memory Limit**: 12GB
- **CPU Limit**: 8 cores
- **Port**: 8507
- **Network**: Isolated bridge network

### Docker Commands

```bash
# Build and start
docker compose up -d

# View logs
docker compose logs -f

# Stop services
docker compose down

# Stop and remove volumes
docker compose down -v
```

## 🧪 Testing

Run tests with Maven:

```bash
mvn test
```

## 📝 Development

### Building from Source

```bash
# Clean and build
mvn clean package

# Skip tests
mvn clean package -DskipTests

# Run locally
mvn spring-boot:run
```

### Adding Dependencies

Edit `pom.xml` and run:
```bash
mvn dependency:resolve
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👤 Author

[@Jouca](https://github.com/Jouca)

## 🙏 Acknowledgments

- [IDFM (Île-de-France Mobilités)](https://www.iledefrance-mobilites.fr/) for providing transit data
- [MobilityData](https://mobilitydata.org/) for GTFS-Realtime specifications
- [OneBusAway](https://onebusaway.org/) for GTFS processing libraries
- [NodeGTFS](https://github.com/BlinkTagInc/node-gtfs) for providing a library for decompressing a GTFS file into a SQLite database

## 📚 Resources

- [GTFS-Realtime Specification](https://gtfs.org/realtime/)
- [SIRI-Lite Documentation](https://www.siri-cen.eu/)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
<br>
- [API for GTFS-RT Trips: Next Departures (Île-de-France Mobilités platform) - global query](https://prim.iledefrance-mobilites.fr/en/apis/idfm-ivtr-requete_globale)
- [API for GTFS-RT Alerts: Traffic Info Messages - Global Query](https://prim.iledefrance-mobilites.fr/en/apis/idfm-disruptions_bulk)

## 🐛 Issues

If you encounter any issues, please file a bug report on the [GitHub Issues](https://github.com/Jouca/IDFM_GTFS-RT/issues) page.

---

**Note**: This is an unofficial project and is not affiliated with IDFM or Île-de-France Mobilités.
