# ---- Build stage ----
FROM eclipse-temurin:25.0.2_10-jdk-jammy AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y maven && apt-get clean

COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

# ---- Runtime stage ----
FROM eclipse-temurin:25.0.2_10-jre-jammy

LABEL project=gtfs-rt

WORKDIR /app

# Install only runtime dependencies (no JDK, no Maven)
RUN apt-get update && apt-get install -y \
    curl \
    bash \
    sqlite3 \
    gnupg \
    && apt-get clean

# Install Node.js 22 from NodeSource
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean

# Install gtfs-import globally using npm
RUN npm install -g gtfs

# Copy only the built jar from the builder stage
COPY --from=builder /app/target/idfm_gtfs_rt-1.0.6.jar target/idfm_gtfs_rt-1.0.6.jar

CMD ["java", "-jar", "target/idfm_gtfs_rt-1.0.6.jar"]
