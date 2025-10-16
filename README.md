<div align="center">
  <img src="logo.png" alt="IDFM GTFS-RT Bridge Logo" width="400"/>
</div>

<div align="center">
   <img src="https://img.shields.io/github/last-commit/Jouca/IDFM_GTFS-RT?display_timestamp=committer&style=for-the-badge&color=ffA500" alt="Last Commit"></img>
   <img src="https://img.shields.io/github/commit-activity/w/Jouca/IDFM_GTFS-RT?style=for-the-badge" alt="Commit Activity"></img>
   <img src="https://img.shields.io/github/commits-since/Jouca/IDFM_GTFS-RT/latest?style=for-the-badge" alt="Commits Since Latest"></img>
   <img src="https://img.shields.io/github/created-at/Jouca/IDFM_GTFS-RT?style=for-the-badge" alt="Created At"></img>
</div>

<div align="center">
   <img src="https://img.shields.io/badge/Java-21-orange?style=for-the-badge&logo=openjdk" alt="Java 21"></img>
   <img src="https://img.shields.io/badge/Spring%20Boot-3.5.6-brightgreen?style=for-the-badge&logo=springboot" alt="Spring Boot"></img>
   <img src="https://img.shields.io/badge/License-MIT-blue?style=for-the-badge" alt="License"></img>
</div>

# IDFM GTFS-RT Bridge

A Spring Boot application that bridges IDFM (Île-de-France Mobilités) real-time transit data to GTFS-Realtime.

## 📋 Overview

This service fetches real-time transit information from the IDFM network and converts it into standardized GTFS-Realtime Protocol Buffer files. It provides REST endpoints to access:

- **GTFS-RT Alerts**: Service disruptions, delays, and transit alerts
- **GTFS-RT Trip Updates**: Real-time vehicle positions, estimated arrival/departure times, and schedule adherence

## 🌐 Free Online Usage

If you want to **directly get feeds messages online**, you can use my **own HTTP links containing the GTFS-RT feeds** !

> #### GTFS-RT Trip Updates : http://gtfsidfm.clarifygdps.com/gtfs-rt-trips-idfm

> #### GTFS-RT Alerts : http://gtfsidfm.clarifygdps.com/gtfs-rt-alerts-idfm

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
- **Spring Boot 3.5.6**: Application framework with scheduling support
- **GTFS Realtime Bindings 0.0.8**: Protocol Buffer handling
- **OneBusAway GTFS 10.2.0**: GTFS data processing
- **SQLite 3.50.3.0**: Local database storage
- **Jackson Databind**: JSON processing
- **OpenCSV 5.12.0**: CSV file parsing
- **Apache Commons DBCP2 2.13.0**: Database connection pooling
- **Maven**: Build and dependency management
- **Docker**: Containerization
- **JaCoCo 0.8.12**: Code coverage reporting

## 📦 Prerequisites

- Java 21 or higher
- Maven 3.6+
- Docker & Docker Compose (for containerized deployment)
- Node.js 22+ (for gtfs-import tool)
- IDFM API Key (required for accessing real-time data)

## 🚀 Quick Start

### GTFS Database Setup

Before running the application, you need to set up the GTFS static data database:

1. **Download GTFS data** from IDFM or specify `GTFS_URL` in your `.env` file
2. **Import GTFS into SQLite** using the gtfs-import tool (Node.js required):
   ```bash
   # Install node-gtfs globally
   npm install -g gtfs
   
   # Import GTFS data
   gtfs-import --gtfsPath=/path/to/gtfs.zip --sqlitePath=gtfs.db
   ```
3. **Place `gtfs.db`** in the project root directory

The application will use this database to match real-time updates with scheduled trips.

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
   java -jar target/idfm_gtfs_rt-1.0.4.jar
   ```

## 🔧 Configuration

Configuration is managed through `application.properties` and environment variables:

```properties
# Application name (configurable via SPRING_APPLICATION_NAME env var)
spring.application.name=${SPRING_APPLICATION_NAME:idfm_gtfs_rt}

# Server port (default: 8507, configurable via SERVER_PORT env var)
server.port=${SERVER_PORT:8507}

# Minutes around now (+/-) to include theoretical trips as CANCELED if missing in realtime
gtfsrt.cancellation.window.minutes=120

# Logging Configuration
logging.level.root=INFO
logging.level.org.jouca.idfm_gtfs_rt=INFO
logging.pattern.console=%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n
```

### Environment Variables

Create a `.env` file with the following variables:

```env
# Spring Application Configuration
SPRING_APPLICATION_NAME=idfm_gtfs_rt

# Server Configuration
SERVER_PORT=8507

# API Configuration
# Your API key for accessing IDFM services
API_KEY=your_api_key_here

# (OPTIONAL) GTFS Data Source
# URL to download the GTFS static data (ZIP file)
GTFS_URL=https://example.com/path/to/gtfs.zip
```

See `.env.example` for a complete template with all available configuration options.

## 📡 API Endpoints

### GET `/gtfs-rt-alerts-idfm`
Download GTFS-RT alerts feed (Protocol Buffer format)

**Response**: Binary `.pb` file containing service alerts

**Example**:
```bash
curl -O http://localhost:8507/gtfs-rt-alerts-idfm
```

---

### GET `/gtfs-rt-trips-idfm`
Download GTFS-RT trip updates feed (Protocol Buffer format)

**Response**: Binary `.pb` file containing trip updates

**Example**:
```bash
curl -O http://localhost:8507/gtfs-rt-trips-idfm
```

---

### POST `/getEntities`
Retrieve specific trip entities by their IDs

**Parameters**:
- `tripIds` (required): Comma-separated list of trip IDs

**Example**:
```bash
curl -X POST "http://localhost:8507/getEntities?tripIds=trip1,trip2,trip3"
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
- **Network**: Isolated bridge network (`gtfs_net`)
- **Restart Policy**: unless-stopped (automatically restarts on failure)

### Docker Commands

```bash
# Build and start
docker compose up -d

# Build without cache
docker compose build --no-cache

# View logs
docker compose logs -f

# Stop services
docker compose down

# Stop and remove volumes
docker compose down -v

# Restart services
docker compose restart
```

### Resource Limits

The Docker container is configured with the following resource limits:
- **Memory**: 12GB
- **CPUs**: 8 cores
- **Restart Policy**: unless-stopped

## 🧪 Testing

Run tests with Maven:

```bash
mvn test
```

### Code Coverage

The project includes JaCoCo for code coverage analysis. After running tests, view the coverage report:

```bash
mvn test
open target/site/jacoco/index.html
```

Coverage reports are available in:
- HTML format: `target/site/jacoco/index.html`
- XML format: `target/site/jacoco/jacoco.xml`
- CSV format: `target/site/jacoco/jacoco.csv`

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
- [API for GTFS-RT Trips: Next Departures (Île-de-France Mobilités platform) – Global Query](https://prim.iledefrance-mobilites.fr/en/apis/idfm-ivtr-requete_globale)
- [API for GTFS-RT Alerts: Traffic Info Messages – Global Query](https://prim.iledefrance-mobilites.fr/en/apis/idfm-disruptions_bulk)

## � Troubleshooting

### Common Issues

**Application won't start**
- Verify Java 21 is installed: `java -version`
- Check if port 8507 is available: `lsof -i :8507`
- Ensure `.env` file is properly configured
- Verify API_KEY is set in `.env` file

**Docker container exits immediately**
- Check container logs: `docker compose logs -f`
- Verify `.env` file exists and is properly formatted
- Ensure sufficient system resources (12GB RAM, 8 CPU cores)

**No data in feeds**
- Verify API_KEY is valid
- Check logs for API connection errors
- Ensure GTFS database (`gtfs.db`) is present and not corrupted
- Verify GTFS_URL (if set) points to a valid GTFS ZIP file

**High memory usage**
- This is expected for large transit networks
- Adjust `mem_limit` in `docker-compose.yml` if needed
- Monitor with: `docker stats gtfs_app`

### Logging

To enable debug logging, update `application.properties`:

```properties
logging.level.org.jouca.idfm_gtfs_rt=DEBUG
```

Or set in `.env`:
```env
LOGGING_LEVEL_ORG_JOUCA_IDFM_GTFS_RT=DEBUG
```

## �🐛 Issues

If you encounter any issues, please file a bug report on the [GitHub Issues](https://github.com/Jouca/IDFM_GTFS-RT/issues) page.

---

**Note**: This is an unofficial project and is not affiliated with IDFM or Île-de-France Mobilités.
