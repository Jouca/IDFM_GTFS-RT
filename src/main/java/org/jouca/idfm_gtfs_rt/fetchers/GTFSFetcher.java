package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.IOException;

/**
 * Utility class for fetching, processing, and importing GTFS (General Transit Feed Specification) data.
 * 
 * <p>This class handles the complete workflow of GTFS data processing:
 * <ul>
 *   <li>Downloading GTFS ZIP files from a remote URL</li>
 *   <li>Extracting the ZIP archive contents</li>
 *   <li>Importing GTFS data into a SQLite database</li>
 *   <li>Processing custom extensions (stop_extensions.txt)</li>
 *   <li>Creating optimized database indexes for query performance</li>
 * </ul>
 * 
 * <p>The class creates multiple indexes to optimize common query patterns used in
 * transit applications, particularly for trip matching and schedule lookups.
 * 
 * @author Jouca
 * @since 1.0
 */
public class GTFSFetcher {

    /**
     * Downloads a GTFS ZIP file from the specified URL, extracts it, imports it into a SQLite database,
     * and creates necessary indexes for optimal query performance.
     * 
     * <p>This method performs the following operations in sequence:
     * <ol>
     *   <li>Downloads the GTFS ZIP file from the provided URL</li>
     *   <li>Extracts the ZIP file contents to a local directory</li>
     *   <li>Imports the GTFS data into a SQLite database using gtfs-import CLI</li>
     *   <li>Imports custom stop extensions from stop_extensions.txt</li>
     *   <li>Creates multiple database indexes for performance optimization</li>
     * </ol>
     * 
     * <p><b>Database Indexes Created:</b>
     * <ul>
     *   <li>idx_object_id - Index on stop_extensions.object_id</li>
     *   <li>idx_object_code - Index on stop_extensions.object_code</li>
     *   <li>idx_stop_times_trip_stop_arrival_departure_seq - Composite index for stop times queries</li>
     *   <li>idx_trips_route_direction_service - Index for trip filtering</li>
     *   <li>idx_calendar_service_dates - Index for calendar date range queries</li>
     *   <li>idx_calendar_service_id - Index for service lookups</li>
     *   <li>idx_calendar_dates_service_date_exception - Composite index for calendar exceptions</li>
     *   <li>idx_calendar_dates_date_exception - Index for date-based exception lookups</li>
     *   <li>idx_stops_stop_id - Index for stop lookups</li>
     *   <li>idx_stop_extensions_object_code - Index for stop extension queries</li>
     *   <li>idx_stop_times_trip_id_stop_sequence - Composite index for ordered stop times</li>
     *   <li>idx_stop_times_stop_id_trip_id_sequence - Composite index for stop-based queries</li>
     *   <li>idx_stop_times_trip_id_stop_id - Composite index for trip-stop lookups</li>
     *   <li>idx_stop_times_trip_id - Index for trip-based queries</li>
     * </ul>
     * 
     * <p><b>External Dependencies:</b>
     * <ul>
     *   <li>gtfs-import CLI tool must be available in the system PATH</li>
     *   <li>sqlite3 CLI tool must be available in the system PATH</li>
     * </ul>
     * 
     * <p><b>File Artifacts:</b>
     * <ul>
     *   <li>IDFM-gtfs.zip - Downloaded ZIP file (temporary)</li>
     *   <li>extracted-gtfs/ - Directory containing extracted GTFS files</li>
     *   <li>outputFilePath - SQLite database file with imported GTFS data</li>
     * </ul>
     *
     * @param urlString The URL of the GTFS ZIP file to download (e.g., IDFM transit data URL)
     * @param outputFilePath The local file path where the SQLite database will be created/updated
     * @throws IOException If an error occurs during:
     *                     <ul>
     *                       <li>Network download</li>
     *                       <li>File extraction or creation</li>
     *                       <li>Database import process</li>
     *                       <li>Index creation</li>
     *                       <li>External process execution (gtfs-import, sqlite3)</li>
     *                     </ul>
     */
    public static void fetchGTFS(String urlString, String outputFilePath) throws IOException {
        // ========================================
        // Step 1: Download GTFS ZIP file from URL
        // ========================================
        // Opens an input stream from the URL and copies the content to a local file.
        // Uses REPLACE_EXISTING to overwrite any previous downloads.
        try (java.io.InputStream in = java.net.URI.create(urlString).toURL().openStream()) {
            java.nio.file.Files.copy(in, java.nio.file.Paths.get("IDFM-gtfs.zip"), 
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IOException("Failed to download GTFS data from " + urlString, e);
        }

        // ===================================
        // Step 2: Extract the ZIP archive
        // ===================================
        // Extracts all files from IDFM-gtfs.zip to the extracted-gtfs/ directory.
        // Maintains directory structure and overwrites existing files.
        try {
            extractZipFile("IDFM-gtfs.zip", java.nio.file.Paths.get("extracted-gtfs"));
        } catch (IOException e) {
            throw new IOException("Failed to unzip GTFS data", e);
        }

        // ================================================================
        // Step 3: Import GTFS data into SQLite database using gtfs-import
        // ================================================================
        // Executes the gtfs-import CLI tool to parse GTFS files and populate the database.
        // The gtfs-import tool creates standard GTFS tables (routes, trips, stops, stop_times, etc.).
        importGtfsData("./IDFM-gtfs.zip", outputFilePath);

        // =============================================================================
        // Step 4: Import custom GTFS extension file (stop_extensions.txt)
        // =============================================================================
        // IDFM provides additional stop metadata in stop_extensions.txt that is not part
        // of standard GTFS. This includes object_id and object_code fields used for
        // matching real-time data with scheduled stops.
        importStopExtensions("./extracted-gtfs/stop_extensions.txt", outputFilePath);

        // ==================================================================================
        // Step 5: Create database indexes for query performance optimization
        // ==================================================================================
        // The following indexes are created to optimize common queries in the application,
        // particularly for real-time trip matching and schedule lookups.
        createDatabaseIndexes(outputFilePath);

        // ==================================================================================
        // Step 6: Completion
        // ==================================================================================
        System.out.println("GTFS data downloaded successfully to: " + outputFilePath);
    }

    /**
     * Executes a shell command and waits for completion.
     * 
     * @param command The command to execute
     * @param errorMessage The error message to use if the command fails
     * @throws IOException If an error occurs during command execution
     */
    private static void executeCommand(String command, String errorMessage) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        
        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException(errorMessage + " (exit code: " + exitCode + ")");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(errorMessage + " (process interrupted)", e);
        }
    }

    /**
     * Creates a database index using sqlite3.
     * 
     * @param outputFilePath The path to the SQLite database
     * @param indexName The name of the index to create
     * @param sql The CREATE INDEX SQL statement
     * @throws IOException If an error occurs during index creation
     */
    private static void createIndex(String outputFilePath, String indexName, String sql) throws IOException {
        System.out.println("Creating index " + indexName + "...");
        String command = "sqlite3 " + outputFilePath + " \"" + sql + "\"";
        executeCommand(command, "Failed to create index " + indexName);
    }

    /**
     * Extracts a ZIP file to a specified directory.
     * 
     * @param zipFilePath Path to the ZIP file
     * @param outputDir Directory to extract files to
     * @throws IOException If an error occurs during extraction
     */
    private static void extractZipFile(String zipFilePath, java.nio.file.Path outputDir) throws IOException {
        java.nio.file.Files.createDirectories(outputDir);

        try (java.util.zip.ZipInputStream zipIn = new java.util.zip.ZipInputStream(
                java.nio.file.Files.newInputStream(java.nio.file.Paths.get(zipFilePath)))) {
            java.util.zip.ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                java.nio.file.Path filePath = outputDir.resolve(entry.getName());
                if (!entry.isDirectory()) {
                    java.nio.file.Files.createDirectories(filePath.getParent());
                    java.nio.file.Files.copy(zipIn, filePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                } else {
                    java.nio.file.Files.createDirectories(filePath);
                }
                zipIn.closeEntry();
            }
        }
    }

    /**
     * Imports GTFS data into SQLite database using gtfs-import CLI tool.
     * 
     * @param gtfsZipPath Path to the GTFS ZIP file
     * @param outputFilePath Path to the SQLite database
     * @throws IOException If an error occurs during import
     */
    private static void importGtfsData(String gtfsZipPath, String outputFilePath) throws IOException {
        String command = "gtfs-import --gtfsPath " + gtfsZipPath + " --sqlitePath " + outputFilePath;
        executeCommand(command, "Failed to import GTFS data");
    }

    /**
     * Imports stop_extensions.txt file into the database.
     * 
     * @param stopExtensionsPath Path to the stop_extensions.txt file
     * @param outputFilePath Path to the SQLite database
     * @throws IOException If an error occurs during import
     */
    private static void importStopExtensions(String stopExtensionsPath, String outputFilePath) throws IOException {
        java.nio.file.Path stopExtensionsFile = java.nio.file.Paths.get(stopExtensionsPath);
        
        if (!java.nio.file.Files.exists(stopExtensionsFile)) {
            throw new IOException("File " + stopExtensionsPath + " does not exist.");
        }

        String command = "sqlite3 " + outputFilePath + " \".mode csv\" \".import " + stopExtensionsPath + " stop_extensions\"";
        executeCommand(command, "Failed to import stop extensions");
    }

    /**
     * Creates all necessary database indexes for query optimization.
     * 
     * @param outputFilePath Path to the SQLite database
     * @throws IOException If an error occurs during index creation
     */
    private static void createDatabaseIndexes(String outputFilePath) throws IOException {
        // Index 1: Fast lookups of stops by object_id
        createIndex(outputFilePath, "idx_object_id", 
            "CREATE INDEX idx_object_id ON stop_extensions (object_id);");

        // Index 2: Fast lookups of stops by object_code
        createIndex(outputFilePath, "idx_object_code", 
            "CREATE INDEX idx_object_code ON stop_extensions (object_code);");

        // Index 3: Composite index for stop_times queries
        createIndex(outputFilePath, "idx_stop_times_trip_stop_arrival_departure_seq", 
            "CREATE INDEX idx_stop_times_trip_stop_arrival_departure_seq ON stop_times (trip_id, stop_id, arrival_timestamp, departure_timestamp, stop_sequence);");

        // Index 4: Optimizes queries filtering trips
        createIndex(outputFilePath, "idx_trips_route_direction_service", 
            "CREATE INDEX idx_trips_route_direction_service ON trips (route_id, direction_id, service_id);");

        // Index 5: Optimizes date range queries in calendar table
        createIndex(outputFilePath, "idx_calendar_service_dates", 
            "CREATE INDEX idx_calendar_service_dates ON calendar (service_id, start_date, end_date);");

        // Index 6: Fast lookups of calendar entries by service_id
        createIndex(outputFilePath, "idx_calendar_service_id", 
            "CREATE INDEX idx_calendar_service_id ON calendar (service_id);");

        // Index 7: Composite index for calendar_dates queries
        createIndex(outputFilePath, "idx_calendar_dates_service_date_exception", 
            "CREATE INDEX idx_calendar_dates_service_date_exception ON calendar_dates (service_id, date, exception_type);");

        // Index 8: Optimizes queries for service exceptions on a specific date
        createIndex(outputFilePath, "idx_calendar_dates_date_exception", 
            "CREATE INDEX idx_calendar_dates_date_exception ON calendar_dates (date, exception_type);");

        // Index 9: Primary index for stop lookups
        createIndex(outputFilePath, "idx_stops_stop_id", 
            "CREATE INDEX idx_stops_stop_id ON stops (stop_id);");

        // Index 10: Fast lookups in stop_extensions by object_code
        createIndex(outputFilePath, "idx_stop_extensions_object_code", 
            "CREATE INDEX idx_stop_extensions_object_code ON stop_extensions (object_code);");

        // Index 11: Composite index for ordered retrieval of stops for a trip
        createIndex(outputFilePath, "idx_stop_times_trip_id_stop_sequence", 
            "CREATE INDEX idx_stop_times_trip_id_stop_sequence ON stop_times (trip_id, stop_sequence);");

        // Index 12: Composite index for finding all trips passing through a stop
        createIndex(outputFilePath, "idx_stop_times_stop_id_trip_id_sequence", 
            "CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id_trip_id_sequence ON stop_times (stop_id, trip_id, stop_sequence);");

        // Index 13: Composite index for trip-stop lookups
        createIndex(outputFilePath, "idx_stop_times_trip_id_stop_id", 
            "CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id_stop_id ON stop_times (trip_id, stop_id);");

        // Index 14: Simple index on trip_id
        createIndex(outputFilePath, "idx_stop_times_trip_id", 
            "CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id ON stop_times (trip_id);");
    }
}