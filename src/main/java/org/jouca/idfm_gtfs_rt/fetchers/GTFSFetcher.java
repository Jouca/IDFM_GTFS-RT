package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for fetching, processing, and importing GTFS (General Transit Feed Specification) data.
 * 
 * <p>This class handles the complete workflow of GTFS data processing:
 * <ul>
 *   <li>Downloading GTFS ZIP files from a remote URL</li>
 *   <li>Extracting the ZIP archive contents</li>
 *   <li>Importing GTFS data into a SQLite database</li>
 *   <li>Processing custom extensions (object_codes_extension.txt)</li>
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
    private static final Logger logger = LoggerFactory.getLogger(GTFSFetcher.class);

    /**
     * Downloads a GTFS ZIP file from the specified URL, extracts it, imports it into a SQLite database,
     * and creates necessary indexes for optimal query performance.
     * 
     * <p>This method performs the following operations in sequence:
     * <ol>
     *   <li>Downloads the GTFS ZIP file from the provided URL</li>
     *   <li>Extracts the ZIP file contents to a local directory</li>
     *   <li>Imports the GTFS data into a SQLite database using gtfs-import CLI</li>
     *   <li>Imports custom object codes extension from object_codes_extension.txt</li>
     *   <li>Creates multiple database indexes for performance optimization</li>
     * </ol>
     * 
     * <p><b>Database Indexes Created:</b>
     * <ul>
     *   <li>idx_object_id - Index on object_codes_extension.object_id</li>
     *   <li>idx_object_code - Index on object_codes_extension.object_code</li>
     *   <li>idx_stop_times_trip_stop_arrival_departure_seq - Composite index for stop times queries</li>
     *   <li>idx_trips_route_direction_service - Index for trip filtering</li>
     *   <li>idx_calendar_service_dates - Index for calendar date range queries</li>
     *   <li>idx_calendar_service_id - Index for service lookups</li>
     *   <li>idx_calendar_dates_service_date_exception - Composite index for calendar exceptions</li>
     *   <li>idx_calendar_dates_date_exception - Index for date-based exception lookups</li>
     *   <li>idx_stops_stop_id - Index for stop lookups</li>
     *   <li>idx_object_codes_extension_object_code - Index for object codes extension queries</li>
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
    private static final String DATA_DIR = "gtfs-data/";

    public static void fetchGTFS(String urlString, String outputFilePath) throws IOException {
        logger.info("Starting GTFS data fetch process...");
        logger.info("Source URL: {}", urlString);
        logger.info("Target database: {}", outputFilePath);

        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(DATA_DIR));

        // ========================================
        // Step 1: Download GTFS ZIP file from URL
        // ========================================
        logger.info("Step 1/5: Downloading GTFS ZIP file...");
        String rawZip = DATA_DIR + "IDFM-gtfs.zip";
        try (java.io.InputStream in = java.net.URI.create(urlString).toURL().openStream()) {
            java.nio.file.Files.copy(in, java.nio.file.Paths.get(rawZip),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            logger.info("GTFS ZIP file downloaded successfully.");
        } catch (IOException e) {
            logger.error("Failed to download GTFS data from {}: {}", urlString, e.getMessage());
        }

        // ===================================
        // Step 1.2: Repair malformed pathways.txt header in downloaded ZIP
        // ===================================
        try {
            repairPathwaysTxt(rawZip);
        } catch (Exception e) {
            logger.warn("Failed to repair pathways.txt header (non-critical): {}", e.getMessage());
        }

        // ===================================
        // Step 1.5: Enrich GTFS with platform codes
        // ===================================
        logger.info("Step 1.5/5: Generating enriched GTFS with platform codes...");
        boolean enrichmentSucceeded = false;
        // Write to a temp file first so the live IDFM-gtfs-enriched.zip is never
        // partially overwritten while a /gtfs download is in progress.
        String enrichedFinal = DATA_DIR + "IDFM-gtfs-enriched.zip";
        String enrichedTemp  = DATA_DIR + "IDFM-gtfs-enriched.zip.new";
        try {
            GTFSEnricher.enrichGTFS(rawZip, enrichedTemp);
            java.nio.file.Files.move(java.nio.file.Paths.get(enrichedTemp),
                    java.nio.file.Paths.get(enrichedFinal),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            logger.info("Enriched GTFS created successfully.");
            enrichmentSucceeded = true;
        } catch (Exception e) {
            logger.warn("GTFS enrichment failed (non-critical, falling back to original ZIP): {}", e.getMessage());
            java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(enrichedTemp));
        }
        String gtfsZipToImport = enrichmentSucceeded ? enrichedFinal : rawZip;

        // ===================================
        // Step 2: Extract the ZIP archive
        // ===================================
        logger.info("Step 2/5: Extracting ZIP archive...");
        String extractDir = DATA_DIR + "extracted-gtfs";
        try {
            extractZipFile(gtfsZipToImport, java.nio.file.Paths.get(extractDir));
            logger.info("ZIP archive extracted successfully.");
        } catch (IOException e) {
            logger.error("Failed to unzip GTFS data: {}", e.getMessage());
        }

        // ================================================================
        // Step 3: Import GTFS data into SQLite database using gtfs-import
        // ================================================================
        logger.info("Step 3/5: Importing GTFS data into SQLite database...");
        importGtfsData(gtfsZipToImport, outputFilePath);
        logger.info("GTFS data imported successfully.");

        // =============================================================================
        // Step 4: Import custom GTFS extension file (object_codes_extension.txt)
        // =============================================================================
        logger.info("Step 4/5: Importing object codes extension...");
        importObjectCodesExtension(extractDir + "/object_codes_extension.txt", outputFilePath);
        logger.info("Object codes extension imported successfully.");

        // ==================================================================================
        // Step 5: Create database indexes for query performance optimization
        // ==================================================================================
        // The following indexes are created to optimize common queries in the application,
        // particularly for real-time trip matching and schedule lookups.
        logger.info("Step 5/5: Creating database indexes...");
        createDatabaseIndexes(outputFilePath);
        logger.info("All database indexes created successfully.");

        // ==================================================================================
        // Step 6: Completion
        // ==================================================================================
        logger.info("GTFS data downloaded successfully to: {}", outputFilePath);
    }

    /**
     * Executes a shell command and waits for completion.
     * 
     * @param command The command to execute
     * @param errorMessage The error message to use if the command fails
     * @throws IOException If an error occurs during command execution
     */
    private static void executeCommand(String command, String errorMessage) throws IOException {
        logger.info("Executing command: {}", command);
        ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logger.info("  > {}", line);
            }
        }
        
        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                logger.error("Command failed with exit code {}: {}", exitCode, command);
                throw new IOException(errorMessage + " (exit code: " + exitCode + ")");
            }
            logger.info("Command completed successfully.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Command interrupted: {}", command);
            throw new IOException(errorMessage + " (process interrupted)", e);
        }
    }

    private static final List<String> PATHWAYS_COLUMNS = List.of(
        "pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional",
        "length", "traversal_time", "stair_count", "max_slope", "min_width",
        "signposted_as", "reversed_signposted_as"
    );

    /**
     * Repairs a malformed pathways.txt in-place inside a GTFS ZIP where the header
     * has fewer columns than the data rows (known issue with the IDFM source feed).
     */
    private static void repairPathwaysTxt(String zipPath) throws IOException {
        Path path = Paths.get(zipPath);
        byte[] pathwaysBytes = null;

        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(path))) {
            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                if ("pathways.txt".equals(entry.getName())) {
                    pathwaysBytes = zin.readAllBytes();
                }
                zin.closeEntry();
            }
        }

        if (pathwaysBytes == null) return;

        List<String> lines = new ArrayList<>(Arrays.asList(
            new String(pathwaysBytes, StandardCharsets.UTF_8).split("\r?\n", -1)));
        // Drop trailing empty lines produced by a terminal newline in the split
        while (!lines.isEmpty() && lines.get(lines.size() - 1).trim().isEmpty()) {
            lines.remove(lines.size() - 1);
        }
        if (lines.size() < 2) return;

        // Find the max column count across all data rows (not just the first)
        int maxCols = 0;
        for (int i = 1; i < lines.size(); i++) {
            int cols = GTFSEnricher.parseCsvLine(lines.get(i)).length;
            if (cols > maxCols) maxCols = cols;
        }
        if (maxCols == 0) return;

        String[] headerFields = GTFSEnricher.parseCsvLine(lines.get(0));
        boolean needsRepair = headerFields.length != maxCols;

        // Pad any data row that has fewer columns than maxCols (e.g. last row missing trailing commas)
        int paddedRows = 0;
        for (int i = 1; i < lines.size(); i++) {
            int cols = GTFSEnricher.parseCsvLine(lines.get(i)).length;
            if (cols < maxCols) {
                StringBuilder sb = new StringBuilder(lines.get(i));
                for (int j = cols; j < maxCols; j++) sb.append(',');
                lines.set(i, sb.toString());
                paddedRows++;
                needsRepair = true;
            }
        }

        if (!needsRepair) return;

        String repairedHeader = String.join(",", PATHWAYS_COLUMNS.subList(0, Math.min(maxCols, PATHWAYS_COLUMNS.size())));
        if (maxCols > PATHWAYS_COLUMNS.size()) {
            for (int i = PATHWAYS_COLUMNS.size(); i < maxCols; i++) repairedHeader += ",extra_" + i;
        }
        lines.set(0, repairedHeader);
        logger.info("Repaired pathways.txt: header {} → {} columns, padded {} short rows",
            headerFields.length, maxCols, paddedRows);

        byte[] repairedBytes = (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8);

        Path tmp = Files.createTempFile("gtfs-repair-", ".zip");
        try {
            try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(path));
                 ZipOutputStream zout = new ZipOutputStream(Files.newOutputStream(tmp))) {
                ZipEntry entry;
                while ((entry = zin.getNextEntry()) != null) {
                    zout.putNextEntry(new ZipEntry(entry.getName()));
                    if ("pathways.txt".equals(entry.getName())) {
                        zout.write(repairedBytes);
                    } else {
                        zin.transferTo(zout);
                    }
                    zout.closeEntry();
                    zin.closeEntry();
                }
            }
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
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
        logger.info("Creating index {}...", indexName);
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
                java.nio.file.Path filePath = outputDir.resolve(entry.getName()).normalize();
                
                // Security check: Prevent Zip Slip attack by ensuring the resolved path
                // is within the intended output directory
                if (!filePath.startsWith(outputDir.normalize())) {
                    logger.warn("Skipping malicious ZIP entry that attempts path traversal: {}", entry.getName());
                    throw new IOException("ZIP entry is outside of the target directory: " + entry.getName());
                }
                
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
     * Imports object_codes_extension.txt file into the database.
     * 
     * <p>This file replaces stop_extensions.txt and contains additional object metadata
     * including an object_type column (stop_area, stop_point, route, trip, agency, etc.).
     * 
     * @param objectCodesExtensionPath Path to the object_codes_extension.txt file
     * @param outputFilePath Path to the SQLite database
     * @throws IOException If an error occurs during import
     */
    private static void importObjectCodesExtension(String objectCodesExtensionPath, String outputFilePath) throws IOException {
        java.nio.file.Path objectCodesExtensionFile = java.nio.file.Paths.get(objectCodesExtensionPath);
        
        if (!java.nio.file.Files.exists(objectCodesExtensionFile)) {
            throw new IOException("File " + objectCodesExtensionPath + " does not exist.");
        }

        String command = "sqlite3 " + outputFilePath + " \".mode csv\" \".import " + objectCodesExtensionPath + " object_codes_extension\"";
        executeCommand(command, "Failed to import object codes extension");
    }

    /**
     * Creates all necessary database indexes for query optimization.
     * 
     * @param outputFilePath Path to the SQLite database
     * @throws IOException If an error occurs during index creation
     */
    private static void createDatabaseIndexes(String outputFilePath) throws IOException {
        // Index 1: Fast lookups of objects by object_id
        createIndex(outputFilePath, "idx_object_id", 
            "CREATE INDEX idx_object_id ON object_codes_extension (object_id);");

        // Index 2: Fast lookups of objects by object_code
        createIndex(outputFilePath, "idx_object_code", 
            "CREATE INDEX idx_object_code ON object_codes_extension (object_code);");

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

        // Index 10: Fast lookups in object_codes_extension by object_code
        createIndex(outputFilePath, "idx_object_codes_extension_object_code", 
            "CREATE INDEX idx_object_codes_extension_object_code ON object_codes_extension (object_code);");

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