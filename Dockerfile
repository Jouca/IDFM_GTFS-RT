FROM openjdk:25-jdk-slim

# Set the working directory
WORKDIR /app

# Install system dependencies (maven, curl, bash, sqlite3, gnupg)
RUN apt-get update && apt-get install -y \
    maven \
    curl \
    bash \
    sqlite3 \
    gnupg \
    && apt-get clean

# Install Node.js 22 + npm depuis NodeSource
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g npm@latest

# Vérification des versions
RUN java -version && node -v && npm -v

# Install gtfs-import globally using npm
RUN npm install -g gtfs

# Copy the Maven build file and the source code
COPY pom.xml .
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests

# Run the application
CMD ["java", "-jar", "target/idfm_gtfs_rt-1.0.1.jar"]
