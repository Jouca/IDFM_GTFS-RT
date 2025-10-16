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
}
