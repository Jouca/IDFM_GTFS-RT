package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.IOException;

public class GTFSFetcher {

    /**
     * Downloads a GTFS ZIP file from the specified URL and saves it to the given file path.
     *
     * @param urlString The URL of the GTFS ZIP file.
     * @param outputFilePath The local file path where the GTFS ZIP file will be saved.
     * @throws IOException If an error occurs during the download or file saving process.
     */
    public static void fetchGTFS(String urlString, String outputFilePath) throws IOException {
        // Download GTFS ZIP file from the URL
        try (java.io.InputStream in = java.net.URI.create(urlString).toURL().openStream()) {
            java.nio.file.Files.copy(in, java.nio.file.Paths.get("IDFM-gtfs.zip"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IOException("Failed to download GTFS data from " + urlString, e);
        }

        // Unzip the downloaded GTFS file with the name IDFM-gtfs.zip
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

        // Run import of GTFS on a SQLite database
        String command = "gtfs-import --gtfsPath ./IDFM-gtfs.zip --sqlitePath " + outputFilePath;
        ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
        processBuilder.redirectErrorStream(true);
        
        Process process = processBuilder.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        
        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("Command exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Process was interrupted", e);
        }

        // Get stop_extensions.txt from the extracted zip and insert it into the SQLite database
        String stopExtensionsPath = "./extracted-gtfs/stop_extensions.txt";
        java.nio.file.Path stopExtensionsFile = java.nio.file.Paths.get(stopExtensionsPath);

        if (!java.nio.file.Files.exists(stopExtensionsFile)) {
            throw new IOException("File " + stopExtensionsPath + " does not exist.");
        }

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

        // Add indexes to the SQLite database
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

        // --- Add the suggested indexes below ---
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

        System.out.println("GTFS data downloaded successfully to: " + outputFilePath);
    }
}