package org.jouca.idfm_gtfs_rt.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TripUpdateGenerator.
 */
class TripUpdateGeneratorTest {

    private TripUpdateGenerator generator;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        // Clear trip states before each test
        TripUpdateGenerator.tripStates.clear();
        TripUpdateGenerator.vehicleToTrip.clear();
        generator = new TripUpdateGenerator();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testTripStateCreation() {
        TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState(
            "trip1", 
            "vehicle1", 
            System.currentTimeMillis() / 1000
        );
        
        assertEquals("trip1", state.tripId);
        assertEquals("vehicle1", state.vehicleId);
        assertTrue(state.lastUpdate > 0);
    }

    @Test
    void testTripStateWithNullValues() {
        TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState(
            null, 
            null, 
            0
        );
        
        assertNull(state.tripId);
        assertNull(state.vehicleId);
        assertEquals(0, state.lastUpdate);
    }

    @Test
    void testTripStatesMapIsInitialized() {
        assertNotNull(TripUpdateGenerator.tripStates);
        assertTrue(TripUpdateGenerator.tripStates.isEmpty());
    }

    @Test
    void testVehicleToTripMapIsInitialized() {
        assertNotNull(TripUpdateGenerator.vehicleToTrip);
        assertTrue(TripUpdateGenerator.vehicleToTrip.isEmpty());
    }

    @Test
    void testAddTripState() {
        long currentTime = System.currentTimeMillis() / 1000;
        TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState(
            "trip1", 
            "vehicle1", 
            currentTime
        );
        
        TripUpdateGenerator.tripStates.put("trip1", state);
        
        assertTrue(TripUpdateGenerator.tripStates.containsKey("trip1"));
        assertEquals(state, TripUpdateGenerator.tripStates.get("trip1"));
    }

    @Test
    void testAddVehicleToTripMapping() {
        TripUpdateGenerator.vehicleToTrip.put("vehicle1", "trip1");
        
        assertTrue(TripUpdateGenerator.vehicleToTrip.containsKey("vehicle1"));
        assertEquals("trip1", TripUpdateGenerator.vehicleToTrip.get("vehicle1"));
    }

    @Test
    void testUpdateTripState() {
        long currentTime = System.currentTimeMillis() / 1000;
        TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState(
            "trip1", 
            "vehicle1", 
            currentTime
        );
        TripUpdateGenerator.tripStates.put("trip1", state);
        
        // Update the state
        state.vehicleId = "vehicle2";
        state.lastUpdate = currentTime + 60;
        
        assertEquals("vehicle2", TripUpdateGenerator.tripStates.get("trip1").vehicleId);
        assertEquals(currentTime + 60, TripUpdateGenerator.tripStates.get("trip1").lastUpdate);
    }

    @Test
    void testClearTripStates() {
        TripUpdateGenerator.tripStates.put("trip1", new TripUpdateGenerator.TripState("trip1", "vehicle1", 0));
        TripUpdateGenerator.tripStates.put("trip2", new TripUpdateGenerator.TripState("trip2", "vehicle2", 0));
        
        assertEquals(2, TripUpdateGenerator.tripStates.size());
        
        TripUpdateGenerator.tripStates.clear();
        
        assertTrue(TripUpdateGenerator.tripStates.isEmpty());
    }

    @Test
    void testMultipleVehicleMappings() {
        TripUpdateGenerator.vehicleToTrip.put("vehicle1", "trip1");
        TripUpdateGenerator.vehicleToTrip.put("vehicle2", "trip2");
        TripUpdateGenerator.vehicleToTrip.put("vehicle3", "trip3");
        
        assertEquals(3, TripUpdateGenerator.vehicleToTrip.size());
        assertEquals("trip1", TripUpdateGenerator.vehicleToTrip.get("vehicle1"));
        assertEquals("trip2", TripUpdateGenerator.vehicleToTrip.get("vehicle2"));
        assertEquals("trip3", TripUpdateGenerator.vehicleToTrip.get("vehicle3"));
    }

    @Test
    void testCheckStopIntegrityWithValidNumericStop() throws Exception {
        String entityJson = """
            {
                "StopPointRef": {
                    "value": "STIF:StopPoint:Q:12345"
                }
            }
            """;
        
        JsonNode entity = objectMapper.readTree(entityJson);
        boolean result = generator.checkStopIntegrity(entity);
        
        assertTrue(result);
    }

    @ParameterizedTest
    @CsvSource({
        "'STIF:StopPoint:Q:ABC123', 'non-numeric stop code'",
        "'STIF:StopPoint:Q:123-ABC', 'special characters in stop code'",
        "'', 'missing stop point value'"
    })
    void testCheckStopIntegrityWithInvalidInputs(String stopPointValue, String description) throws Exception {
        String entityJson;
        if (stopPointValue.isEmpty()) {
            entityJson = """
                {
                    "StopPointRef": {}
                }
                """;
        } else {
            entityJson = String.format("""
                {
                    "StopPointRef": {
                        "value": "%s"
                    }
                }
                """, stopPointValue);
        }
        
        JsonNode entity = objectMapper.readTree(entityJson);
        boolean result = generator.checkStopIntegrity(entity);
        
        assertFalse(result, "Expected false for: " + description);
    }

    @Test
    void testCheckStopIntegrityWithMissingStopPointRef() throws Exception {
        String entityJson = "{}";
        
        JsonNode entity = objectMapper.readTree(entityJson);
        boolean result = generator.checkStopIntegrity(entity);
        
        assertFalse(result);
    }

    @Test
    void testTripStateUpdateWithSameTripId() {
        long time1 = System.currentTimeMillis() / 1000;
        TripUpdateGenerator.TripState state1 = new TripUpdateGenerator.TripState("trip1", "vehicle1", time1);
        TripUpdateGenerator.tripStates.put("trip1", state1);
        
        long time2 = time1 + 100;
        TripUpdateGenerator.TripState state2 = new TripUpdateGenerator.TripState("trip1", "vehicle2", time2);
        TripUpdateGenerator.tripStates.put("trip1", state2);
        
        assertEquals(1, TripUpdateGenerator.tripStates.size());
        assertEquals("vehicle2", TripUpdateGenerator.tripStates.get("trip1").vehicleId);
        assertEquals(time2, TripUpdateGenerator.tripStates.get("trip1").lastUpdate);
    }

    @Test
    void testVehicleToTripRemapping() {
        TripUpdateGenerator.vehicleToTrip.put("vehicle1", "trip1");
        assertEquals("trip1", TripUpdateGenerator.vehicleToTrip.get("vehicle1"));
        
        // Remap the same vehicle to a different trip
        TripUpdateGenerator.vehicleToTrip.put("vehicle1", "trip2");
        assertEquals("trip2", TripUpdateGenerator.vehicleToTrip.get("vehicle1"));
        assertEquals(1, TripUpdateGenerator.vehicleToTrip.size());
    }

    @Test
    void testTripStateTimestampValidation() {
        long futureTime = (System.currentTimeMillis() / 1000) + 10000;
        TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState("trip1", "vehicle1", futureTime);
        
        assertEquals(futureTime, state.lastUpdate);
        assertTrue(state.lastUpdate > System.currentTimeMillis() / 1000);
    }

    @Test
    void testTripStatePastTimestamp() {
        long pastTime = (System.currentTimeMillis() / 1000) - 10000;
        TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState("trip1", "vehicle1", pastTime);
        
        assertEquals(pastTime, state.lastUpdate);
        assertTrue(state.lastUpdate < System.currentTimeMillis() / 1000);
    }

    @Test
    void testMultipleTripStatesManagement() {
        long currentTime = System.currentTimeMillis() / 1000;
        
        for (int i = 1; i <= 5; i++) {
            TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState(
                "trip" + i, 
                "vehicle" + i, 
                currentTime + i
            );
            TripUpdateGenerator.tripStates.put("trip" + i, state);
        }
        
        assertEquals(5, TripUpdateGenerator.tripStates.size());
        
        for (int i = 1; i <= 5; i++) {
            assertTrue(TripUpdateGenerator.tripStates.containsKey("trip" + i));
            assertEquals("vehicle" + i, TripUpdateGenerator.tripStates.get("trip" + i).vehicleId);
        }
    }

    @Test
    void testVehicleToTripClearAndRepopulate() {
        TripUpdateGenerator.vehicleToTrip.put("vehicle1", "trip1");
        TripUpdateGenerator.vehicleToTrip.put("vehicle2", "trip2");
        
        assertEquals(2, TripUpdateGenerator.vehicleToTrip.size());
        
        TripUpdateGenerator.vehicleToTrip.clear();
        assertTrue(TripUpdateGenerator.vehicleToTrip.isEmpty());
        
        TripUpdateGenerator.vehicleToTrip.put("vehicle3", "trip3");
        assertEquals(1, TripUpdateGenerator.vehicleToTrip.size());
        assertEquals("trip3", TripUpdateGenerator.vehicleToTrip.get("vehicle3"));
    }

    @Test
    void testCheckStopIntegrityWithEdgeCases() throws Exception {
        // Test with zero
        String entityJson1 = """
            {
                "StopPointRef": {
                    "value": "STIF:StopPoint:Q:0"
                }
            }
            """;
        JsonNode entity1 = objectMapper.readTree(entityJson1);
        assertTrue(generator.checkStopIntegrity(entity1));
        
        // Test with large number
        String entityJson2 = """
            {
                "StopPointRef": {
                    "value": "STIF:StopPoint:Q:999999999"
                }
            }
            """;
        JsonNode entity2 = objectMapper.readTree(entityJson2);
        assertTrue(generator.checkStopIntegrity(entity2));
    }

    @Test
    void testTripStateEquality() {
        long currentTime = System.currentTimeMillis() / 1000;
        TripUpdateGenerator.TripState state1 = new TripUpdateGenerator.TripState("trip1", "vehicle1", currentTime);
        TripUpdateGenerator.TripState state2 = new TripUpdateGenerator.TripState("trip1", "vehicle1", currentTime);
        
        // Check that fields are equal
        assertEquals(state1.tripId, state2.tripId);
        assertEquals(state1.vehicleId, state2.vehicleId);
        assertEquals(state1.lastUpdate, state2.lastUpdate);
    }

    @Test
    void testConcurrentTripStateModification() {
        TripUpdateGenerator.TripState state = new TripUpdateGenerator.TripState("trip1", "vehicle1", 0);
        TripUpdateGenerator.tripStates.put("trip1", state);
        
        // Simulate concurrent modification
        state.vehicleId = "vehicle2";
        assertEquals("vehicle2", TripUpdateGenerator.tripStates.get("trip1").vehicleId);
        
        state.lastUpdate = 1000;
        assertEquals(1000, TripUpdateGenerator.tripStates.get("trip1").lastUpdate);
    }

    @Test
    void testRealtimeDirectionStatsAddTrip() {
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        
        // Create mock TripMeta
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 1, 3600, "20231122");
        
        stats.addTrip(meta);
        
        assertTrue(stats.tripIds.contains("trip1"));
        assertEquals(3600, stats.maxStartTime);
    }

    @Test
    void testRealtimeDirectionStatsAddTripUpdatesMaxStartTime() {
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta1 = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 1, 3600, "20231122");
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta2 = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip2", "route1", 1, 7200, "20231122");
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta3 = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip3", "route1", 1, 5400, "20231122");
        
        stats.addTrip(meta1);
        assertEquals(3600, stats.maxStartTime);
        
        stats.addTrip(meta2);
        assertEquals(7200, stats.maxStartTime);
        
        stats.addTrip(meta3);
        assertEquals(7200, stats.maxStartTime); // Should remain 7200
    }

    @Test
    void testRealtimeDirectionStatsAddTripWithNull() {
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        
        stats.addTrip(null);
        
        assertTrue(stats.tripIds.isEmpty());
        assertEquals(Long.MIN_VALUE, stats.maxStartTime);
    }

    @Test
    void testRealtimeDirectionStatsMultipleTrips() {
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        
        for (int i = 1; i <= 5; i++) {
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta = 
                new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip" + i, "route1", 1, i * 1000, "20231122");
            stats.addTrip(meta);
        }
        
        assertEquals(5, stats.tripIds.size());
        assertEquals(5000, stats.maxStartTime);
    }

    @Test
    void testProcessingContextInitialization() {
        TripUpdateGenerator.ProcessingContext context = new TripUpdateGenerator.ProcessingContext();
        
        assertNotNull(context.statsByRouteDirection);
        assertTrue(context.statsByRouteDirection.isEmpty());
    }

    @Test
    void testProcessingContextAddRouteDirection() {
        TripUpdateGenerator.ProcessingContext context = new TripUpdateGenerator.ProcessingContext();
        
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        java.util.concurrent.ConcurrentHashMap<Integer, TripUpdateGenerator.RealtimeDirectionStats> directionMap = 
            new java.util.concurrent.ConcurrentHashMap<>();
        directionMap.put(0, stats);
        
        context.statsByRouteDirection.put("route1", directionMap);
        
        assertEquals(1, context.statsByRouteDirection.size());
        assertTrue(context.statsByRouteDirection.containsKey("route1"));
        assertNotNull(context.statsByRouteDirection.get("route1").get(0));
    }

    @Test
    void testCheckStopIntegrityWithEmptyString() throws Exception {
        String entityJson = """
            {
                "StopPointRef": {
                    "value": "STIF:StopPoint:Q:"
                }
            }
            """;
        
        JsonNode entity = objectMapper.readTree(entityJson);
        
        // This should throw an ArrayIndexOutOfBoundsException when splitting
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            generator.checkStopIntegrity(entity);
        });
    }

    @Test
    void testCheckStopIntegrityWithLeadingZeros() throws Exception {
        String entityJson = """
            {
                "StopPointRef": {
                    "value": "STIF:StopPoint:Q:00123"
                }
            }
            """;
        
        JsonNode entity = objectMapper.readTree(entityJson);
        boolean result = generator.checkStopIntegrity(entity);
        
        assertTrue(result); // Leading zeros are still valid integers
    }

    @Test
    void testRenderProgressBarEdgeCases() throws Exception {
        // Use reflection to call private method
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "renderProgressBar", int.class, int.class);
        method.setAccessible(true);
        
        // Test with total = 0 (should return early)
        assertDoesNotThrow(() -> method.invoke(generator, 0, 0));
        
        // Test with negative values
        assertDoesNotThrow(() -> method.invoke(generator, -5, 10));
        
        // Test with current > total
        assertDoesNotThrow(() -> method.invoke(generator, 15, 10));
        
        // Test normal progress
        assertDoesNotThrow(() -> method.invoke(generator, 5, 10));
        
        // Test completion
        assertDoesNotThrow(() -> method.invoke(generator, 10, 10));
    }

    @Test
    void testParseTimeMethod() throws Exception {
        // Use reflection to test private parseTime method
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseTime", String.class);
        method.setAccessible(true);
        
        // Test valid ISO timestamp
        String isoTime = "2025-10-16T10:30:00Z";
        long result = (long) method.invoke(generator, isoTime);
        assertTrue(result > 0);
        
        // Test that cache is used (call again with same timestamp)
        long cachedResult = (long) method.invoke(generator, isoTime);
        assertEquals(result, cachedResult);
    }

    @Test
    void testParseTimeWithDifferentTimestamps() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseTime", String.class);
        method.setAccessible(true);
        
        String time1 = "2025-10-16T10:00:00Z";
        String time2 = "2025-10-16T11:00:00Z";
        
        long result1 = (long) method.invoke(generator, time1);
        long result2 = (long) method.invoke(generator, time2);
        
        // Second timestamp should be 3600 seconds later
        assertEquals(3600, result2 - result1);
    }

    @Test
    void testIsValidContextWithNullContext() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isValidContext", TripUpdateGenerator.ProcessingContext.class);
        method.setAccessible(true);
        
        boolean result = (boolean) method.invoke(generator, (TripUpdateGenerator.ProcessingContext) null);
        assertFalse(result);
    }

    @Test
    void testIsValidContextWithEmptyContext() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isValidContext", TripUpdateGenerator.ProcessingContext.class);
        method.setAccessible(true);
        
        TripUpdateGenerator.ProcessingContext context = new TripUpdateGenerator.ProcessingContext();
        boolean result = (boolean) method.invoke(generator, context);
        assertFalse(result);
    }

    @Test
    void testIsValidContextWithValidContext() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isValidContext", TripUpdateGenerator.ProcessingContext.class);
        method.setAccessible(true);
        
        TripUpdateGenerator.ProcessingContext context = new TripUpdateGenerator.ProcessingContext();
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        java.util.concurrent.ConcurrentHashMap<Integer, TripUpdateGenerator.RealtimeDirectionStats> directionMap = 
            new java.util.concurrent.ConcurrentHashMap<>();
        directionMap.put(0, stats);
        context.statsByRouteDirection.put("route1", directionMap);
        
        boolean result = (boolean) method.invoke(generator, context);
        assertTrue(result);
    }

    @Test
    void testIsValidDirectionStatsWithNull() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isValidDirectionStats", TripUpdateGenerator.RealtimeDirectionStats.class);
        method.setAccessible(true);
        
        boolean result = (boolean) method.invoke(generator, (TripUpdateGenerator.RealtimeDirectionStats) null);
        assertFalse(result);
    }

    @Test
    void testIsValidDirectionStatsWithEmptyStats() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isValidDirectionStats", TripUpdateGenerator.RealtimeDirectionStats.class);
        method.setAccessible(true);
        
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        boolean result = (boolean) method.invoke(generator, stats);
        assertFalse(result);
    }

    @Test
    void testIsValidDirectionStatsWithValidStats() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isValidDirectionStats", TripUpdateGenerator.RealtimeDirectionStats.class);
        method.setAccessible(true);
        
        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        stats.tripIds.add("trip1");
        
        boolean result = (boolean) method.invoke(generator, stats);
        assertTrue(result);
    }

    @Test
    void testIsTripCanceledWhenTripIsCanceled() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isTripCanceled", 
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta.class,
            long.class,
            java.util.Set.class);
        method.setAccessible(true);
        
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 1, 3600, "20231122");
        
        java.util.Set<String> realtimeTripIds = new java.util.HashSet<>();
        realtimeTripIds.add("trip2");
        realtimeTripIds.add("trip3");
        
        boolean result = (boolean) method.invoke(generator, meta, 7200L, realtimeTripIds);
        assertTrue(result); // Trip started before cutoff and not in realtime data
    }

    @Test
    void testIsTripCanceledWhenTripIsNotCanceled() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isTripCanceled", 
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta.class,
            long.class,
            java.util.Set.class);
        method.setAccessible(true);
        
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 1, 3600, "20231122");
        
        java.util.Set<String> realtimeTripIds = new java.util.HashSet<>();
        realtimeTripIds.add("trip1");
        realtimeTripIds.add("trip2");
        
        boolean result = (boolean) method.invoke(generator, meta, 7200L, realtimeTripIds);
        assertFalse(result); // Trip is in realtime data
    }

    @Test
    void testIsTripCanceledWhenTripStartsAfterCutoff() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isTripCanceled", 
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta.class,
            long.class,
            java.util.Set.class);
        method.setAccessible(true);
        
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 1, 9000, "20231122");
        
        java.util.Set<String> realtimeTripIds = new java.util.HashSet<>();
        
        boolean result = (boolean) method.invoke(generator, meta, 7200L, realtimeTripIds);
        assertFalse(result); // Trip starts after cutoff
    }

    @Test
    void testBuildCanceledEntity() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "buildCanceledEntity", 
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta.class);
        method.setAccessible(true);
        
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip123", "route456", 1, 3600, "20231122");
        
        com.google.transit.realtime.GtfsRealtime.FeedEntity entity = 
            (com.google.transit.realtime.GtfsRealtime.FeedEntity) method.invoke(generator, meta);
        
        assertNotNull(entity);
        assertEquals("trip123", entity.getId());
        assertTrue(entity.hasTripUpdate());
        
        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate = entity.getTripUpdate();
        assertEquals("trip123", tripUpdate.getTrip().getTripId());
        assertEquals("route456", tripUpdate.getTrip().getRouteId());
        assertEquals(1, tripUpdate.getTrip().getDirectionId());
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED, 
                    tripUpdate.getTrip().getScheduleRelationship());
    }

    @Test
    void testParseDirectionFromSimpleValueAller() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromSimpleValue", String.class);
        method.setAccessible(true);
        
        assertEquals(1, (int) method.invoke(generator, "Aller"));
        assertEquals(1, (int) method.invoke(generator, "inbound"));
        assertEquals(1, (int) method.invoke(generator, "A"));
    }

    @Test
    void testParseDirectionFromSimpleValueRetour() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromSimpleValue", String.class);
        method.setAccessible(true);
        
        assertEquals(0, (int) method.invoke(generator, "Retour"));
        assertEquals(0, (int) method.invoke(generator, "outbound"));
        assertEquals(0, (int) method.invoke(generator, "R"));
    }

    @Test
    void testParseDirectionFromSimpleValueInvalid() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromSimpleValue", String.class);
        method.setAccessible(true);
        
        assertEquals(-1, (int) method.invoke(generator, "Unknown"));
        assertEquals(-1, (int) method.invoke(generator, ""));
        assertEquals(-1, (int) method.invoke(generator, "B"));
    }

    @Test
    void testParseDirectionFromColonDelimitedValueValid() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromColonDelimitedValue", String.class);
        method.setAccessible(true);
        
        assertEquals(1, (int) method.invoke(generator, "IDFM:Line:123:A"));
        assertEquals(0, (int) method.invoke(generator, "IDFM:Line:123:R"));
    }

    @Test
    void testParseDirectionFromColonDelimitedValueInvalid() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromColonDelimitedValue", String.class);
        method.setAccessible(true);
        
        assertEquals(-1, (int) method.invoke(generator, "IDFM:Line:123"));
        assertEquals(-1, (int) method.invoke(generator, "IDFM:Line:123:X"));
        assertEquals(-1, (int) method.invoke(generator, ":::"));
    }

    @Test
    void testExtractTimeFromCallWithAllFields() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractTimeFromCall", JsonNode.class);
        method.setAccessible(true);
        
        // Test with ExpectedArrivalTime (highest priority)
        String jsonWithExpectedArrival = """
            {
                "ExpectedArrivalTime": "2025-10-16T10:30:00Z",
                "ExpectedDepartureTime": "2025-10-16T10:35:00Z",
                "AimedArrivalTime": "2025-10-16T10:28:00Z",
                "AimedDepartureTime": "2025-10-16T10:33:00Z"
            }
            """;
        JsonNode node = objectMapper.readTree(jsonWithExpectedArrival);
        String result = (String) method.invoke(generator, node);
        assertEquals("2025-10-16T10:30:00Z", result);
    }

    @Test
    void testExtractTimeFromCallWithOnlyDeparture() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractTimeFromCall", JsonNode.class);
        method.setAccessible(true);
        
        String jsonWithDeparture = """
            {
                "ExpectedDepartureTime": "2025-10-16T10:35:00Z"
            }
            """;
        JsonNode node = objectMapper.readTree(jsonWithDeparture);
        String result = (String) method.invoke(generator, node);
        assertEquals("2025-10-16T10:35:00Z", result);
    }

    @Test
    void testExtractTimeFromCallWithNoTimes() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractTimeFromCall", JsonNode.class);
        method.setAccessible(true);
        
        String jsonEmpty = "{}";
        JsonNode node = objectMapper.readTree(jsonEmpty);
        String result = (String) method.invoke(generator, node);
        assertNull(result);
    }

    @Test
    void testResolveDirectionIdFromTripMeta() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveDirectionId", 
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta.class,
            Integer.class,
            String.class);
        method.setAccessible(true);
        
        org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta meta = 
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 1, 3600, "20231122");
        
        int result = (int) method.invoke(generator, meta, null, "trip1");
        assertEquals(1, result); // Should use tripMeta.directionId
    }

    @Test
    void testResolveDirectionIdFromMatching() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveDirectionId", 
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta.class,
            Integer.class,
            String.class);
        method.setAccessible(true);
        
        int result = (int) method.invoke(generator, null, 0, "trip1");
        assertEquals(0, result); // Should use directionIdForMatching
    }

    @Test
    void testResolveDirectionIdDefaultValue() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveDirectionId", 
            org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta.class,
            Integer.class,
            String.class);
        method.setAccessible(true);
        
        int result = (int) method.invoke(generator, null, null, "unknownTrip");
        assertEquals(0, result); // Should default to 0
    }

    @Test
    void testExtractExistingEntityIds() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractExistingEntityIds", 
            com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class);
        method.setAccessible(true);
        
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage = 
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        
        feedMessage.addEntity(com.google.transit.realtime.GtfsRealtime.FeedEntity.newBuilder()
            .setId("trip1")
            .build());
        feedMessage.addEntity(com.google.transit.realtime.GtfsRealtime.FeedEntity.newBuilder()
            .setId("trip2")
            .build());
        
        @SuppressWarnings("unchecked")
        java.util.Set<String> result = (java.util.Set<String>) method.invoke(generator, feedMessage);
        
        assertEquals(2, result.size());
        assertTrue(result.contains("trip1"));
        assertTrue(result.contains("trip2"));
    }

    @Test
    void testExtractCallTimeForSortingWithExpectedArrival() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractCallTimeForSorting", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "ExpectedArrivalTime": "2025-10-16T10:30:00Z"
            }
            """;
        JsonNode call = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, call);
        
        assertTrue(result > 0);
        assertTrue(result < Long.MAX_VALUE);
    }

    @Test
    void testExtractCallTimeForSortingWithExpectedDeparture() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractCallTimeForSorting", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "ExpectedDepartureTime": "2025-10-16T11:00:00Z"
            }
            """;
        JsonNode call = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, call);
        
        assertTrue(result > 0);
    }

    @Test
    void testExtractCallTimeForSortingWithAimedTimes() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractCallTimeForSorting", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedArrivalTime": "2025-10-16T10:30:00Z",
                "AimedDepartureTime": "2025-10-16T10:35:00Z"
            }
            """;
        JsonNode call = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, call);
        
        assertTrue(result > 0);
    }

    @Test
    void testExtractCallTimeForSortingWithNoTimes() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractCallTimeForSorting", JsonNode.class);
        method.setAccessible(true);
        
        String json = "{}";
        JsonNode call = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, call);
        
        assertEquals(Long.MAX_VALUE, result);
    }

    @Test
    void testIsEstimatedCallInPastWithFutureExpectedArrival() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isEstimatedCallInPast", JsonNode.class);
        method.setAccessible(true);
        
        // Set future time (1 hour from now)
        String futureTime = java.time.Instant.now().plusSeconds(3600).toString();
        String json = String.format("""
            {
                "ExpectedArrivalTime": "%s"
            }
            """, futureTime);
        
        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);
        
        assertFalse(result);
    }

    @Test
    void testIsEstimatedCallInPastWithPastExpectedArrival() throws Exception {
        // Set currentEpochSecond to a value in the future (relative to 2020)
        java.lang.reflect.Field field = TripUpdateGenerator.class.getDeclaredField("currentEpochSecond");
        field.setAccessible(true);
        field.setLong(generator, System.currentTimeMillis() / 1000);
        
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isEstimatedCallInPast", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "ExpectedArrivalTime": "2020-01-01T10:00:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);
        
        assertTrue(result);
    }

    @Test
    void testIsEstimatedCallInPastWithAimedTimes() throws Exception {
        // Set currentEpochSecond to a value in the future (relative to 2020)
        java.lang.reflect.Field field = TripUpdateGenerator.class.getDeclaredField("currentEpochSecond");
        field.setAccessible(true);
        field.setLong(generator, System.currentTimeMillis() / 1000);
        
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isEstimatedCallInPast", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedArrivalTime": "2020-01-01T10:00:00Z",
                "AimedDepartureTime": "2020-01-01T10:05:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);
        
        assertTrue(result);
    }

    @Test
    void testIsEstimatedCallInPastWithOnlyDepartureTimes() throws Exception {
        // Set currentEpochSecond to a value in the future (relative to 2020)
        java.lang.reflect.Field field = TripUpdateGenerator.class.getDeclaredField("currentEpochSecond");
        field.setAccessible(true);
        field.setLong(generator, System.currentTimeMillis() / 1000);
        
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isEstimatedCallInPast", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "ExpectedDepartureTime": "2020-01-01T10:00:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);
        
        assertTrue(result);
    }

    @Test
    void testSetArrivalTimeWithExpectedArrivalTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setArrivalTime", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "ExpectedArrivalTime": "2025-10-16T10:30:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        method.invoke(generator, call, builder);
        
        assertTrue(builder.hasArrival());
        assertTrue(builder.getArrival().getTime() > 0);
    }

    @Test
    void testSetArrivalTimeWithAimedArrivalTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setArrivalTime", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedArrivalTime": "2025-10-16T10:30:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        method.invoke(generator, call, builder);
        
        assertTrue(builder.hasArrival());
    }

    @Test
    void testSetArrivalTimeWithNoArrivalTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setArrivalTime", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = "{}";
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        method.invoke(generator, call, builder);
        
        assertFalse(builder.hasArrival());
    }

    @Test
    void testSetDepartureTimeWithExpectedDepartureTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setDepartureTime", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "ExpectedDepartureTime": "2025-10-16T10:35:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        method.invoke(generator, call, builder);
        
        assertTrue(builder.hasDeparture());
        assertTrue(builder.getDeparture().getTime() > 0);
    }

    @Test
    void testSetDepartureTimeWithAimedDepartureTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setDepartureTime", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedDepartureTime": "2025-10-16T10:35:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        method.invoke(generator, call, builder);
        
        assertTrue(builder.hasDeparture());
    }

    @Test
    void testSetDepartureTimeWithNoDepartureTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setDepartureTime", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = "{}";
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        method.invoke(generator, call, builder);
        
        assertFalse(builder.hasDeparture());
    }

    @Test
    void testHandleCancellationStatusWithDepartureCancelled() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "handleCancellationStatus", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DepartureStatus": "CANCELLED"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        builder.setArrival(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(1000).build());
        builder.setDeparture(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(1100).build());
        
        method.invoke(generator, call, builder);
        
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED, 
                     builder.getScheduleRelationship());
        assertFalse(builder.hasArrival());
        assertFalse(builder.hasDeparture());
    }

    @Test
    void testHandleCancellationStatusWithArrivalCancelled() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "handleCancellationStatus", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "ArrivalStatus": "CANCELLED"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        builder.setArrival(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(1000).build());
        
        method.invoke(generator, call, builder);
        
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED, 
                     builder.getScheduleRelationship());
    }

    @Test
    void testHandleCancellationStatusWithBothCancelled() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "handleCancellationStatus", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DepartureStatus": "CANCELLED",
                "ArrivalStatus": "CANCELLED"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        method.invoke(generator, call, builder);
        
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED, 
                     builder.getScheduleRelationship());
    }

    @Test
    void testHandleCancellationStatusWithNoCancellation() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "handleCancellationStatus", 
            JsonNode.class, 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DepartureStatus": "ON_TIME",
                "ArrivalStatus": "DELAYED"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = 
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        builder.setArrival(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(1000).build());
        
        method.invoke(generator, call, builder);
        
        assertTrue(builder.hasArrival());
    }

    @Test
    void testExtractTimeFromCallWithAimedArrival() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractTimeFromCall", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedArrivalTime": "2025-10-16T10:30:00Z"
            }
            """;
        JsonNode node = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, node);
        
        assertEquals("2025-10-16T10:30:00Z", result);
    }

    @Test
    void testExtractTimeFromCallWithAimedDeparture() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractTimeFromCall", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedDepartureTime": "2025-10-16T10:35:00Z"
            }
            """;
        JsonNode node = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, node);
        
        assertEquals("2025-10-16T10:35:00Z", result);
    }

    @Test
    void testExtractTimeFromCallPriority() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractTimeFromCall", JsonNode.class);
        method.setAccessible(true);
        
        // ExpectedArrivalTime should have highest priority
        String json = """
            {
                "ExpectedArrivalTime": "2025-10-16T10:30:00Z",
                "ExpectedDepartureTime": "2025-10-16T10:35:00Z",
                "AimedArrivalTime": "2025-10-16T10:28:00Z",
                "AimedDepartureTime": "2025-10-16T10:33:00Z"
            }
            """;
        JsonNode node = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, node);
        
        assertEquals("2025-10-16T10:30:00Z", result);
    }

    @Test
    void testParseDirectionFromNameWithAller() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": [
                    {
                        "value": "Aller"
                    }
                ]
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(1, result);
    }

    @Test
    void testParseDirectionFromNameWithRetour() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": [
                    {
                        "value": "Retour"
                    }
                ]
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(0, result);
    }

    @Test
    void testParseDirectionFromNameWithInbound() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": [
                    {
                        "value": "inbound"
                    }
                ]
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(1, result);
    }

    @Test
    void testParseDirectionFromNameWithOutbound() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": [
                    {
                        "value": "outbound"
                    }
                ]
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(0, result);
    }

    @Test
    void testParseDirectionFromNameWithSingleLetterA() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": [
                    {
                        "value": "A"
                    }
                ]
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(0, result);
    }

    @Test
    void testParseDirectionFromNameWithSingleLetterR() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": [
                    {
                        "value": "R"
                    }
                ]
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(1, result);
    }

    @Test
    void testParseDirectionFromNameWithMissingField() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = "{}";
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(-1, result);
    }

    @Test
    void testParseDirectionFromNameWithEmptyArray() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromName", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": []
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(-1, result);
    }

    @Test
    void testParseDirectionFromRefWithMissingField() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromRef", JsonNode.class);
        method.setAccessible(true);
        
        String json = "{}";
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(-1, result);
    }

    @Test
    void testParseDirectionFromRefWithSimpleValue() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromRef", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionRef": {
                    "value": "Aller"
                }
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(1, result);
    }

    @Test
    void testDetermineDirectionWithValidRef() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "determineDirection", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionRef": {
                    "value": "IDFM:Line:123:A"
                }
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(1, result);
    }

    @Test
    void testDetermineDirectionWithValidName() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "determineDirection", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "DirectionName": [
                    {
                        "value": "Aller"
                    }
                ]
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(1, result);
    }

    @Test
    void testDetermineDirectionWithNoValidFields() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "determineDirection", JsonNode.class);
        method.setAccessible(true);
        
        String json = "{}";
        JsonNode entity = objectMapper.readTree(json);
        int result = (int) method.invoke(generator, entity);
        
        assertEquals(-1, result);
    }

    @Test
    void testGroupTheoreticalTripsByRouteAndDirection() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "groupTheoreticalTripsByRouteAndDirection", java.util.List.class);
        method.setAccessible(true);
        
        java.util.List<org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta> trips = new java.util.ArrayList<>();
        trips.add(new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 0, 3600, "20231122"));
        trips.add(new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip2", "route1", 1, 7200, "20231122"));
        trips.add(new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip3", "route2", 0, 5400, "20231122"));
        
        @SuppressWarnings("unchecked")
        java.util.Map<String, java.util.Map<Integer, java.util.List<org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta>>> result = 
            (java.util.Map<String, java.util.Map<Integer, java.util.List<org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta>>>) 
            method.invoke(generator, trips);
        
        assertEquals(2, result.size());
        assertTrue(result.containsKey("route1"));
        assertTrue(result.containsKey("route2"));
        assertEquals(2, result.get("route1").size());
        assertEquals(1, result.get("route2").size());
    }

    @Test
    void testExtractFirstCallTimeWithAllFields() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractFirstCallTime", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "EstimatedCalls": {
                    "EstimatedCall": [
                        {
                            "ExpectedDepartureTime": "2025-10-16T10:00:00Z"
                        }
                    ]
                }
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, entity);
        
        assertTrue(result > 0);
        assertTrue(result < Long.MAX_VALUE);
    }

    @Test
    void testExtractFirstCallTimeWithNoEstimatedCalls() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractFirstCallTime", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "EstimatedCalls": {
                    "EstimatedCall": []
                }
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, entity);
        
        assertEquals(Long.MAX_VALUE, result);
    }

    @Test
    void testExtractFirstCallTimeWithOnlyAimedTimes() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractFirstCallTime", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "EstimatedCalls": {
                    "EstimatedCall": [
                        {
                            "AimedDepartureTime": "2025-10-16T10:00:00Z"
                        }
                    ]
                }
            }
            """;
        JsonNode entity = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, entity);
        
        assertTrue(result > 0);
    }

    @Test
    void testIndexedEntityRecordCreation() throws Exception {
        Class<?> indexedEntityClass = Class.forName("org.jouca.idfm_gtfs_rt.generator.TripUpdateGenerator$IndexedEntity");
        java.lang.reflect.Constructor<?> constructor = indexedEntityClass.getDeclaredConstructor(
            int.class, 
            com.google.transit.realtime.GtfsRealtime.FeedEntity.class);
        constructor.setAccessible(true);
        
        com.google.transit.realtime.GtfsRealtime.FeedEntity entity = 
            com.google.transit.realtime.GtfsRealtime.FeedEntity.newBuilder()
                .setId("test_trip")
                .build();
        
        Object indexedEntity = constructor.newInstance(5, entity);
        
        assertNotNull(indexedEntity);
        
        java.lang.reflect.Method indexMethod = indexedEntityClass.getDeclaredMethod("index");
        indexMethod.setAccessible(true);
        assertEquals(5, (int) indexMethod.invoke(indexedEntity));
        
        java.lang.reflect.Method entityMethod = indexedEntityClass.getDeclaredMethod("entity");
        entityMethod.setAccessible(true);
        assertEquals(entity, entityMethod.invoke(indexedEntity));
    }

    @Test
    void testParseDirectionFromColonDelimitedValueWithShortString() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "parseDirectionFromColonDelimitedValue", String.class);
        method.setAccessible(true);
        
        assertEquals(-1, (int) method.invoke(generator, "A:B"));
        assertEquals(-1, (int) method.invoke(generator, ""));
    }

    @Test
    void testExtractCallTimeForSortingOnlyAimedDeparture() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "extractCallTimeForSorting", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedDepartureTime": "2025-10-16T11:00:00Z"
            }
            """;
        JsonNode call = objectMapper.readTree(json);
        long result = (long) method.invoke(generator, call);
        
        assertTrue(result > 0);
        assertTrue(result < Long.MAX_VALUE);
    }

    @Test
    void testIsEstimatedCallInPastWithFutureDeparture() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isEstimatedCallInPast", JsonNode.class);
        method.setAccessible(true);
        
        String futureTime = java.time.Instant.now().plusSeconds(3600).toString();
        String json = String.format("""
            {
                "ExpectedDepartureTime": "%s"
            }
            """, futureTime);
        
        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);
        
        assertFalse(result);
    }

    @Test
    void testIsEstimatedCallInPastWithPastAimedDeparture() throws Exception {
        // Set currentEpochSecond to a value in the future (relative to 2020)
        java.lang.reflect.Field field = TripUpdateGenerator.class.getDeclaredField("currentEpochSecond");
        field.setAccessible(true);
        field.setLong(generator, System.currentTimeMillis() / 1000);
        
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isEstimatedCallInPast", JsonNode.class);
        method.setAccessible(true);
        
        String json = """
            {
                "AimedDepartureTime": "2020-01-01T10:00:00Z"
            }
            """;
        
        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);
        
        assertTrue(result);
    }
}
