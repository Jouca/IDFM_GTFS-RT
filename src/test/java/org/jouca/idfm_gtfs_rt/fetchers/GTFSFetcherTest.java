package org.jouca.idfm_gtfs_rt.fetchers;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GTFSFetcher class.
 * 
 * Tests the GTFS data fetching, extraction, and import functionality.
 */
class GTFSFetcherTest {

    @TempDir
    Path tempDir;

    private Path testDbPath;

    @BeforeEach
    void setUp() {
        testDbPath = tempDir.resolve("test-gtfs.db");
    }

    @AfterEach
    void tearDown() throws IOException {
        // Cleanup test files
        if (Files.exists(testDbPath)) {
            Files.delete(testDbPath);
        }
    }

    @Test
    void testFetchGTFSWithInvalidUrl() {
        // Test that invalid URL throws IOException
        assertThrows(Exception.class, () -> 
            GTFSFetcher.fetchGTFS("invalid://url", testDbPath.toString())
        );
    }

    @Test
    void testFetchGTFSWithNullUrl() {
        // Test that null URL throws exception
        assertThrows(Exception.class, () -> 
            GTFSFetcher.fetchGTFS(null, testDbPath.toString())
        );
    }

    @Test
    void testFetchGTFSWithNullOutputPath() {
        // Test that null output path throws exception
        assertThrows(Exception.class, () -> 
            GTFSFetcher.fetchGTFS("https://example.com/gtfs.zip", null)
        );
    }

    @Test
    void testFetchGTFSWithEmptyUrl() {
        // Test that empty URL throws exception
        assertThrows(Exception.class, () -> 
            GTFSFetcher.fetchGTFS("", testDbPath.toString())
        );
    }

    @Test
    void testFetchGTFSWithEmptyOutputPath() {
        // Test that empty output path throws exception
        assertThrows(Exception.class, () ->
            GTFSFetcher.fetchGTFS("https://example.com/gtfs.zip", "")
        );
    }

    // --- repairStopTimesTxt: fixes trips (e.g. flex/demand-responsive lines) whose final
    // stop_time has no arrival/departure at all, which otherwise crashes OTP's graph builder
    // outright ("missing final stop time") for the whole feed, not just that one trip. ---

    private java.lang.reflect.Method repairStopTimesTxtMethod() throws Exception {
        java.lang.reflect.Method method = GTFSFetcher.class.getDeclaredMethod("repairStopTimesTxt", String.class);
        method.setAccessible(true);
        return method;
    }

    private Path writeTestZip(String stopTimesContent, String tripsContent) throws IOException {
        Path zipPath = tempDir.resolve("test-gtfs-" + System.nanoTime() + ".zip");
        try (java.util.zip.ZipOutputStream zout = new java.util.zip.ZipOutputStream(Files.newOutputStream(zipPath))) {
            zout.putNextEntry(new java.util.zip.ZipEntry("stop_times.txt"));
            zout.write(stopTimesContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zout.closeEntry();

            zout.putNextEntry(new java.util.zip.ZipEntry("trips.txt"));
            zout.write(tripsContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zout.closeEntry();
        }
        return zipPath;
    }

    private String readZipEntry(Path zipPath, String entryName) throws IOException {
        try (java.util.zip.ZipInputStream zin = new java.util.zip.ZipInputStream(Files.newInputStream(zipPath))) {
            java.util.zip.ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                if (entryName.equals(entry.getName())) {
                    return new String(zin.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                }
            }
        }
        throw new IOException("Entry not found: " + entryName);
    }

    @Test
    void testRepairStopTimesTxtFixesTripWithMissingFinalStopTime() throws Exception {
        // Reproduces the real reported case: a demand-responsive trip whose first stop has a
        // real time and every following stop (including the last) has none at all.
        String stopTimes = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + "trip1,08:00:00,08:00:00,STOP_A,0\n"
            + "trip1,,,STOP_B,1\n"
            + "trip1,,,STOP_C,2\n"
            + "trip1,,,STOP_D,3\n";
        Path zipPath = writeTestZip(stopTimes, "trip_id,route_id\ntrip1,route1\n");

        repairStopTimesTxtMethod().invoke(null, zipPath.toString());

        String[] lines = readZipEntry(zipPath, "stop_times.txt").split("\n");
        assertEquals(5, lines.length, "row count must be unchanged (header + 4 rows)");
        // The three previously-empty middle/last rows must still be present, untouched except
        // for the final one.
        assertEquals("trip1,,,STOP_B,1", lines[2]);
        assertEquals("trip1,,,STOP_C,2", lines[3]);
        String[] lastRow = lines[4].split(",", -1);
        assertEquals("STOP_D", lastRow[3]);
        assertEquals("3", lastRow[4]);
        // 3 stops since the last known time (08:00:00) at 60s each -> 08:03:00.
        assertEquals("08:03:00", lastRow[1], "synthetic arrival must be 3 gaps of 60s after the last known time");
        assertEquals("08:03:00", lastRow[2], "synthetic departure must match the synthetic arrival");
    }

    @Test
    void testRepairStopTimesTxtLeavesWellFormedTripsUntouched() throws Exception {
        String stopTimes = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + "trip1,08:00:00,08:00:00,STOP_A,0\n"
            + "trip1,08:05:00,08:05:00,STOP_B,1\n"
            + "trip2,09:00:00,09:00:00,STOP_A,0\n"
            + "trip2,09:10:00,09:10:00,STOP_B,1\n";
        Path zipPath = writeTestZip(stopTimes, "trip_id,route_id\ntrip1,route1\ntrip2,route1\n");

        repairStopTimesTxtMethod().invoke(null, zipPath.toString());

        assertEquals(stopTimes.trim(), readZipEntry(zipPath, "stop_times.txt").trim(),
            "a feed with no broken trips must be left byte-for-byte unchanged");
    }

    @Test
    void testRepairStopTimesTxtPreservesOtherZipEntries() throws Exception {
        String stopTimes = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + "trip1,08:00:00,08:00:00,STOP_A,0\n"
            + "trip1,,,STOP_B,1\n";
        String trips = "trip_id,route_id\ntrip1,route1\n";
        Path zipPath = writeTestZip(stopTimes, trips);

        repairStopTimesTxtMethod().invoke(null, zipPath.toString());

        assertEquals(trips, readZipEntry(zipPath, "trips.txt"), "unrelated entries must pass through unchanged");
    }

    @Test
    void testRepairStopTimesTxtSkipsTripWithNoRealTimeAtAll() throws Exception {
        // Degenerate case (not expected in practice): nothing to anchor a synthetic time on, so
        // the trip must be left as-is rather than fabricating a time from nothing.
        String stopTimes = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + "trip1,,,STOP_A,0\n"
            + "trip1,,,STOP_B,1\n";
        Path zipPath = writeTestZip(stopTimes, "trip_id,route_id\ntrip1,route1\n");

        repairStopTimesTxtMethod().invoke(null, zipPath.toString());

        assertEquals(stopTimes.trim(), readZipEntry(zipPath, "stop_times.txt").trim());
    }
}
