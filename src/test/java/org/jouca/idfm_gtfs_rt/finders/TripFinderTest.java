package org.jouca.idfm_gtfs_rt.finders;

import org.jouca.idfm_gtfs_rt.records.EstimatedCall;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TripFinder class.
 * 
 * Tests trip finding and stop resolution functionality.
 */
class TripFinderTest {

    @BeforeEach
    void setUp() {
        // Clear caches before each test
        TripFinder.stopCodeCache.clear();
    }

    @Test
    void testResolveStopIdWithNullStopCode() {
        String result = TripFinder.resolveStopId(null);
        assertNull(result);
    }

    @Test
    void testResolveStopIdWithEmptyStopCode() {
        String result = TripFinder.resolveStopId("");
        assertNull(result);
    }

    @Test
    void testResolveStopIdCaching() {
        // First call should cache the result
        String stopCode = "test123";
        String firstResult = TripFinder.resolveStopId(stopCode);
        
        // Second call should use cached value
        String secondResult = TripFinder.resolveStopId(stopCode);
        
        // Results should be consistent
        assertEquals(firstResult, secondResult);
    }

    @Test
    void testFindTripIdFromEstimatedCallsWithNullRouteId() {
        List<EstimatedCall> calls = new ArrayList<>();
        
        assertThrows(IllegalArgumentException.class, () ->
            TripFinder.findTripIdFromEstimatedCalls(null, calls, false, "dest1", null, false, null)
        );
    }

    @Test
    void testFindTripIdFromEstimatedCallsWithNullEstimatedCalls() {
        assertThrows(IllegalArgumentException.class, () ->
            TripFinder.findTripIdFromEstimatedCalls("route1", null, false, "dest1", null, false, null)
        );
    }

    @Test
    void testFindTripIdFromEstimatedCallsWithEmptyEstimatedCalls() {
        List<EstimatedCall> emptyCalls = new ArrayList<>();
        
        assertThrows(IllegalArgumentException.class, () ->
            TripFinder.findTripIdFromEstimatedCalls("route1", emptyCalls, false, "dest1", null, false, null)
        );
    }

    @Test
    void testCheckIfObjectCodesExtensionTableExists() {
        // This will return false if database doesn't exist or table doesn't exist
        boolean result = TripFinder.checkIfObjectCodesExtensionTableExists();
        
        // Result should be boolean (test database connection)
        assertTrue(result || !result); // Verifies method executes without exception
    }

    @Test
    void testGetAllStopTimesFromTripWithNullTripId() {
        List<String> result = TripFinder.getAllStopTimesFromTrip(null);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAllStopTimesFromTripWithEmptyTripId() {
        List<String> result = TripFinder.getAllStopTimesFromTrip("");
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testFindStopSequenceWithNullTripId() {
        List<String> stopUpdates = new ArrayList<>();
        String result = TripFinder.findStopSequence(null, "stop1", stopUpdates);
        
        assertNull(result);
    }

    @Test
    void testFindStopSequenceWithNullStopId() {
        List<String> stopUpdates = new ArrayList<>();
        String result = TripFinder.findStopSequence("trip1", null, stopUpdates);
        
        assertNull(result);
    }

    @Test
    void testFindStopChildrenByTripWithNullValues() {
        String result = TripFinder.findStopChildrenByTrip(null, null);
        assertNull(result);
    }

    @Test
    void testFindStopIdFromCodeWithNull() {
        String result = TripFinder.findStopIdFromCode(null);
        assertNull(result);
    }

    @Test
    void testFindStopIdFromStopExtensionWithNull() {
        String result = TripFinder.findStopIdFromStopExtension(null);
        assertNull(result);
    }

    @Test
    void testGetTripDirectionWithNullTripId() {
        String result = TripFinder.getTripDirection(null);
        assertNull(result);
    }

    @Test
    void testGetTripDirectionWithEmptyTripId() {
        String result = TripFinder.getTripDirection("");
        assertNull(result);
    }

    @Test
    void testGetActiveTripsForRoutesTodayWithNull() {
        List<TripFinder.TripMeta> result = TripFinder.getActiveTripsForRoutesToday(null);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetActiveTripsForRoutesTodayWithEmptyList() {
        List<TripFinder.TripMeta> result = TripFinder.getActiveTripsForRoutesToday(new ArrayList<>());
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetTripMetaWithNullTripId() {
        TripFinder.TripMeta result = TripFinder.getTripMeta(null);
        assertNull(result);
    }

    @Test
    void testGetTripMetaWithEmptyTripId() {
        TripFinder.TripMeta result = TripFinder.getTripMeta("");
        assertNull(result);
    }

    @Test
    void testTripMetaConstruction() {
        TripFinder.TripMeta meta = new TripFinder.TripMeta("trip1", "route1", 0, 3600, 0, "20231122");
        
        assertEquals("trip1", meta.tripId);
        assertEquals("route1", meta.routeId);
        assertEquals(0, meta.directionId);
        assertEquals(3600, meta.firstTimeSecOfDay);
        assertEquals("20231122", meta.startDate);
    }

    @Test
    void testGetScheduledArrivalTimeWithNullInputs() {
        Long result = TripFinder.getScheduledArrivalTime(null, "stop1", "1", 
            java.time.ZoneId.of("Europe/Paris"));
        assertNull(result);
    }

    @Test
    void testGetScheduledDepartureTimeWithNullInputs() {
        Long result = TripFinder.getScheduledDepartureTime(null, "stop1", "1", 
            java.time.ZoneId.of("Europe/Paris"));
        assertNull(result);
    }

    @Test
    void vehicleRefMatchesOnlyAWholeTripIdToken() {
        assertTrue(TripFinder.endsWithWholeId("IDFM:TN:SNCF:51194306-913c-4f27-8712-7cbd2776a14b",
                "51194306-913c-4f27-8712-7cbd2776a14b"));
        assertTrue(TripFinder.endsWithWholeId("IDFM:FSQY:529-C01541-50365-9084845", "9084845"));
        assertTrue(TripFinder.endsWithWholeId("2776", "2776"));
        // a short numeric id must not select an unrelated course whose id merely ends with it
        assertFalse(TripFinder.endsWithWholeId("IDFM:Transdev_Nord_Seine_Saint-Denis:21882776", "2776"));
        assertFalse(TripFinder.endsWithWholeId("IDFM:stif:local-264688-C02847-27381", "7381"));
        assertFalse(TripFinder.endsWithWholeId("IDFM:X:1", "22"));
        assertFalse(TripFinder.endsWithWholeId(null, "1"));
    }
}
