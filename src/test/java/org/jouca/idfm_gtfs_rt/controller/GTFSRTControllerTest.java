package org.jouca.idfm_gtfs_rt.controller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GTFSRTController class.
 * 
 * Tests the REST API endpoints for serving GTFS-RT data.
 */
class GTFSRTControllerTest {

    private GTFSRTController controller;
    private Path testAlertsFile;
    private Path testTripsFile;
    private Path testEntitiesFile;
    private Path testSiriLiteFile;

    @BeforeEach
    void setUp() throws IOException {
        controller = new GTFSRTController();
        
        // Create test files with sample data
        testAlertsFile = Paths.get("gtfs-rt-alerts-idfm.pb");
        testTripsFile = Paths.get("gtfs-rt-trips-idfm.pb");
        testEntitiesFile = Paths.get("entities_trips.json");
        testSiriLiteFile = Paths.get("sirilite_data.json");
        
        Files.write(testAlertsFile, "test alert data".getBytes());
        Files.write(testTripsFile, "test trip data".getBytes());
        Files.write(testEntitiesFile, "{\"trip1\": {\"data\": \"value\"}}".getBytes());
        Files.write(testSiriLiteFile, "{\"test\": \"data\"}".getBytes());
    }

    @AfterEach
    void tearDown() throws IOException {
        // Cleanup test files
        Files.deleteIfExists(testAlertsFile);
        Files.deleteIfExists(testTripsFile);
        Files.deleteIfExists(testEntitiesFile);
        Files.deleteIfExists(testSiriLiteFile);
    }

    @Test
    void testGetGtfsRtAlertFileExists() throws Exception {
        ResponseEntity<byte[]> response = controller.getGtfsRtAlertFile();
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().length > 0);
    }

    @Test
    void testGetGtfsRtTripFileExists() throws Exception {
        ResponseEntity<byte[]> response = controller.getGtfsRtTripFile();
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().length > 0);
    }

    @Test
    void testGetGtfsRtAlertFileNotFound() throws Exception {
        Files.delete(testAlertsFile);
        
        ResponseEntity<byte[]> response = controller.getGtfsRtAlertFile();
        
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void testGetGtfsRtTripFileNotFound() throws Exception {
        Files.delete(testTripsFile);
        
        ResponseEntity<byte[]> response = controller.getGtfsRtTripFile();
        
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void testGetEntityWithValidTripIds() {
        ResponseEntity<String> response = controller.getEntity("trip1");
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("trip1"));
    }

    @Test
    void testGetEntityWithMultipleTripIds() {
        ResponseEntity<String> response = controller.getEntity("trip1,trip2");
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    void testGetEntityWithNullTripIds() {
        ResponseEntity<String> response = controller.getEntity(null);
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void testGetEntityWithEmptyTripIds() {
        ResponseEntity<String> response = controller.getEntity("");
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void testGetEntityFileNotFound() throws IOException {
        Files.delete(testEntitiesFile);
        
        ResponseEntity<String> response = controller.getEntity("trip1");
        
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    }

    @Test
    void testGetSiriLiteFileExists() throws Exception {
        ResponseEntity<byte[]> response = controller.getSiriLiteFile();
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().length > 0);
    }

    @Test
    void testGetSiriLiteFileNotFound() throws Exception {
        Files.delete(testSiriLiteFile);
        
        ResponseEntity<byte[]> response = controller.getSiriLiteFile();
        
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }
}
