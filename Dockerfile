FROM openjdk:21-jdk-slim

# Set the working directory
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    maven \
    curl \
    bash \
    sqlite3 \
    npm \
    && apt-get clean

# Install gtfs-import globally using npm
RUN npm install -g gtfs

# Copy the Maven build file and the source code
COPY pom.xml .
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests

# Run the application
CMD ["java", "-jar", "target/idfm_gtfs_rt-0.0.1-SNAPSHOT.jar"]