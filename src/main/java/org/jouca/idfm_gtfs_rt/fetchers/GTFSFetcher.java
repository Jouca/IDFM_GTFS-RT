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
            java.nio.file.Files.copy(in, java.nio.file.Paths.get("IDFM-gtfs.zip"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IOException("Failed to download GTFS data from " + urlString, e);
        }

        // ===================================
        // Step 2: Extract the ZIP archive
        // ===================================
        // Extracts all files from IDFM-gtfs.zip to the extracted-gtfs/ directory.
        // Maintains directory structure and overwrites existing files.
        java.nio.file.Path outputDir = java.nio.file.Paths.get("extracted-gtfs");
        java.nio.file.Files.createDirectories(outputDir);

        try (java.util.zip.ZipInputStream zipIn = new java.util.zip.ZipInputStream(java.nio.file.Files.newInputStream(java.nio.file.Paths.get("IDFM-gtfs.zip")))) {
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
        } catch (IOException e) {
            throw new IOException("Failed to unzip GTFS data", e);
        }

        // ================================================================
        // Step 3: Import GTFS data into SQLite database using gtfs-import
        // ================================================================
        // Executes the gtfs-import CLI tool to parse GTFS files and populate the database.
        // The gtfs-import tool creates standard GTFS tables (routes, trips, stops, stop_times, etc.).
        String command = "gtfs-import --gtfsPath ./IDFM-gtfs.zip --sqlitePath " + outputFilePath;
        ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
        processBuilder.redirectErrorStream(true);
        
        Process process = processBuilder.start();
        // Stream and display the output from the gtfs-import process
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        
        // Wait for the process to complete and check exit code
        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("Command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Process was interrupted", e);
        }

        // =============================================================================
        // Step 4: Import custom GTFS extension file (stop_extensions.txt)
        // =============================================================================
        // IDFM provides additional stop metadata in stop_extensions.txt that is not part
        // of standard GTFS. This includes object_id and object_code fields used for
        // matching real-time data with scheduled stops.
        String stopExtensionsPath = "./extracted-gtfs/stop_extensions.txt";
        java.nio.file.Path stopExtensionsFile = java.nio.file.Paths.get(stopExtensionsPath);

        if (!java.nio.file.Files.exists(stopExtensionsFile)) {
            throw new IOException("File " + stopExtensionsPath + " does not exist.");
        }

        // Import stop_extensions.txt into the database using sqlite3 CSV import
        String insertCommand = "sqlite3 " + outputFilePath + " \".mode csv\" \".import " + stopExtensionsPath + " stop_extensions\"";
        ProcessBuilder insertProcessBuilder = new ProcessBuilder("/bin/sh", "-c", insertCommand);
        insertProcessBuilder.redirectErrorStream(true);
        Process insertProcess = insertProcessBuilder.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(insertProcess.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = insertProcess.waitFor();
            if (exitCode != 0) {
            throw new IOException("Insert command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Insert process was interrupted", e);
        }

        // ==================================================================================
        // Step 5: Create database indexes for query performance optimization
        // ==================================================================================
        // The following indexes are created to optimize common queries in the application,
        // particularly for real-time trip matching and schedule lookups.
        
        // --- Index 1: idx_object_id ---
        // Purpose: Fast lookups of stops by object_id in stop_extensions table
        System.out.println("Creating index idx_object_id...");
        String indexCommand = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_object_id ON stop_extensions (object_id);\"";
        ProcessBuilder indexProcessBuilder = new ProcessBuilder("/bin/sh", "-c", indexCommand);
        indexProcessBuilder.redirectErrorStream(true);
        Process indexProcess = indexProcessBuilder.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 2: idx_object_code ---
        // Purpose: Fast lookups of stops by object_code in stop_extensions table
        System.out.println("Creating index idx_object_code...");
        String indexCommand2 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_object_code ON stop_extensions (object_code);\"";
        ProcessBuilder indexProcessBuilder2 = new ProcessBuilder("/bin/sh", "-c", indexCommand2);
        indexProcessBuilder2.redirectErrorStream(true);
        Process indexProcess2 = indexProcessBuilder2.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess2.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess2.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 3: idx_stop_times_trip_stop_arrival_departure_seq ---
        // Purpose: Composite index for stop_times queries involving trip_id, stop_id,
        // arrival/departure timestamps, and stop_sequence. Critical for real-time matching.
        System.out.println("Creating index idx_stop_times_trip_stop_arrival_departure_seq...");
        String indexCommand3 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_stop_times_trip_stop_arrival_departure_seq\n" + //
                "ON stop_times (trip_id, stop_id, arrival_timestamp, departure_timestamp, stop_sequence);\"";
        ProcessBuilder indexProcessBuilder3 = new ProcessBuilder("/bin/sh", "-c", indexCommand3);
        indexProcessBuilder3.redirectErrorStream(true);
        Process indexProcess3 = indexProcessBuilder3.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess3.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess3.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 4: idx_trips_route_direction_service ---
        // Purpose: Optimizes queries filtering trips by route_id, direction_id, and service_id.
        // Essential for finding active trips on specific routes and directions.
        System.out.println("Creating index idx_trips_route_direction_service...");
        String indexCommand4 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_trips_route_direction_service ON trips (route_id, direction_id, service_id);\"";
        ProcessBuilder indexProcessBuilder4 = new ProcessBuilder("/bin/sh", "-c", indexCommand4);
        indexProcessBuilder4.redirectErrorStream(true);
        Process indexProcess4 = indexProcessBuilder4.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess4.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess4.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 5: idx_calendar_service_dates ---
        // Purpose: Optimizes date range queries in the calendar table.
        // Used to determine if a service is active on a given date.
        System.out.println("Creating index idx_calendar_service_dates...");
        String indexCommand5 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_calendar_service_dates ON calendar (service_id, start_date, end_date);\"";
        ProcessBuilder indexProcessBuilder5 = new ProcessBuilder("/bin/sh", "-c", indexCommand5);
        indexProcessBuilder5.redirectErrorStream(true);
        Process indexProcess5 = indexProcessBuilder5.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess5.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess5.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 6: idx_calendar_service_id ---
        // Purpose: Fast lookups of calendar entries by service_id.
        System.out.println("Creating index idx_calendar_service_id...");
        String indexCommand6 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_calendar_service_id ON calendar (service_id);\"";
        ProcessBuilder indexProcessBuilder6 = new ProcessBuilder("/bin/sh", "-c", indexCommand6);
        indexProcessBuilder6.redirectErrorStream(true);
        Process indexProcess6 = indexProcessBuilder6.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess6.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess6.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 7: idx_calendar_dates_service_date_exception ---
        // Purpose: Composite index for calendar_dates queries by service_id, date, and exception_type.
        // Used to find service exceptions (added or removed dates) for specific services.
        System.out.println("Creating index idx_calendar_dates_service_date_exception...");
        String indexCommand7 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_calendar_dates_service_date_exception ON calendar_dates (service_id, date, exception_type);\"";
        ProcessBuilder indexProcessBuilder7 = new ProcessBuilder("/bin/sh", "-c", indexCommand7);
        indexProcessBuilder7.redirectErrorStream(true);
        Process indexProcess7 = indexProcessBuilder7.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess7.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess7.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 8: idx_calendar_dates_date_exception ---
        // Purpose: Optimizes queries for all service exceptions on a specific date.
        System.out.println("Creating index idx_calendar_dates_date_exception...");
        String indexCommand8 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_calendar_dates_date_exception ON calendar_dates (date, exception_type);\"";
        ProcessBuilder indexProcessBuilder8 = new ProcessBuilder("/bin/sh", "-c", indexCommand8);
        indexProcessBuilder8.redirectErrorStream(true);
        Process indexProcess8 = indexProcessBuilder8.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess8.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess8.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 9: idx_stops_stop_id ---
        // Purpose: Primary index for stop lookups by stop_id in the stops table.
        System.out.println("Creating index idx_stops_stop_id...");
        String indexCommand9 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_stops_stop_id ON stops (stop_id);\"";
        ProcessBuilder indexProcessBuilder9 = new ProcessBuilder("/bin/sh", "-c", indexCommand9);
        indexProcessBuilder9.redirectErrorStream(true);
        Process indexProcess9 = indexProcessBuilder9.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess9.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess9.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 10: idx_stop_extensions_object_code ---
        // Purpose: Fast lookups in stop_extensions by object_code (duplicate of index 2, defensive).
        System.out.println("Creating index idx_stop_extensions_object_code...");
        String indexCommand10 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_stop_extensions_object_code ON stop_extensions (object_code);\"";
        ProcessBuilder indexProcessBuilder10 = new ProcessBuilder("/bin/sh", "-c", indexCommand10);
        indexProcessBuilder10.redirectErrorStream(true);
        Process indexProcess10 = indexProcessBuilder10.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess10.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
            System.out.println(line);
            }
        }

        try {
            int exitCode = indexProcess10.waitFor();
            if (exitCode != 0) {
            throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 11: idx_stop_times_trip_id_stop_sequence ---
        // Purpose: Composite index for ordered retrieval of stops for a trip.
        // Critical for generating trip updates with stops in the correct sequence.
        System.out.println("Creating index idx_stop_times_trip_id_stop_sequence...");
        String indexCommand11 = "sqlite3 " + outputFilePath + " \"CREATE INDEX idx_stop_times_trip_id_stop_sequence ON stop_times (trip_id, stop_sequence);\"";
        ProcessBuilder indexProcessBuilder11 = new ProcessBuilder("/bin/sh", "-c", indexCommand11);
        indexProcessBuilder11.redirectErrorStream(true);
        Process indexProcess11 = indexProcessBuilder11.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess11.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        try {
            int exitCode = indexProcess11.waitFor();
            if (exitCode != 0) {
                throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 12: idx_stop_times_stop_id_trip_id_sequence ---
        // Purpose: Composite index for finding all trips passing through a stop.
        // Useful for stop-based queries in real-time updates.
        // Note: Uses CREATE INDEX IF NOT EXISTS to avoid errors on re-runs.
        System.out.println("Creating index idx_stop_times_stop_id_trip_id_sequence...");
        String indexCommand12 = "sqlite3 " + outputFilePath + " \"CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id_trip_id_sequence ON stop_times (stop_id, trip_id, stop_sequence);\"";
        ProcessBuilder indexProcessBuilder12 = new ProcessBuilder("/bin/sh", "-c", indexCommand12);
        indexProcessBuilder12.redirectErrorStream(true);
        Process indexProcess12 = indexProcessBuilder12.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess12.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        try {
            int exitCode = indexProcess12.waitFor();
            if (exitCode != 0) {
                throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 13: idx_stop_times_trip_id_stop_id ---
        // Purpose: Composite index for queries involving both trip_id and stop_id.
        // Optimizes lookups for specific stop within a specific trip.
        System.out.println("Creating index idx_stop_times_trip_id_stop_id...");
        String indexCommand13 = "sqlite3 " + outputFilePath + " \"CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id_stop_id ON stop_times (trip_id, stop_id);\"";
        ProcessBuilder indexProcessBuilder13 = new ProcessBuilder("/bin/sh", "-c", indexCommand13);
        indexProcessBuilder13.redirectErrorStream(true);
        Process indexProcess13 = indexProcessBuilder13.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess13.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        try {
            int exitCode = indexProcess13.waitFor();
            if (exitCode != 0) {
                throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // --- Index 14: idx_stop_times_trip_id ---
        // Purpose: Simple index on trip_id for general stop_times queries by trip.
        // Most fundamental index for trip-based operations.
        System.out.println("Creating index idx_stop_times_trip_id...");
        String indexCommand14 = "sqlite3 " + outputFilePath + " \"CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id ON stop_times (trip_id);\"";
        ProcessBuilder indexProcessBuilder14 = new ProcessBuilder("/bin/sh", "-c", indexCommand14);
        indexProcessBuilder14.redirectErrorStream(true);
        Process indexProcess14 = indexProcessBuilder14.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(indexProcess14.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        try {
            int exitCode = indexProcess14.waitFor();
            if (exitCode != 0) {
                throw new IOException("Index command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index process was interrupted", e);
        }

        // ==================================================================================
        // Step 6: Completion
        // ==================================================================================
        System.out.println("GTFS data downloaded successfully to: " + outputFilePath);
    }
}