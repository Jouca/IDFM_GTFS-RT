package org.jouca.idfm_gtfs_rt.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jouca.idfm_gtfs_rt.finders.TripFinder;
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
            new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 1, 3600, 0, "20231122");
        
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
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class,
            long.class);
        method.setAccessible(true);

        String json = """
            {
                "ExpectedArrivalTime": "2025-10-16T10:30:00Z"
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder =
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        method.invoke(generator, call, builder, Long.MIN_VALUE);

        assertTrue(builder.hasArrival());
        assertTrue(builder.getArrival().getTime() > 0);
    }

    @Test
    void testSetArrivalTimeWithAimedArrivalTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setArrivalTime",
            JsonNode.class,
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class,
            long.class);
        method.setAccessible(true);

        String json = """
            {
                "AimedArrivalTime": "2025-10-16T10:30:00Z"
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder =
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        method.invoke(generator, call, builder, Long.MIN_VALUE);

        assertTrue(builder.hasArrival());
    }

    @Test
    void testSetArrivalTimeWithNoArrivalTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setArrivalTime",
            JsonNode.class,
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class,
            long.class);
        method.setAccessible(true);

        String json = "{}";

        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder =
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        method.invoke(generator, call, builder, Long.MIN_VALUE);

        assertFalse(builder.hasArrival());
    }

    @Test
    void testSetDepartureTimeWithExpectedDepartureTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setDepartureTime",
            JsonNode.class,
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class,
            long.class);
        method.setAccessible(true);

        String json = """
            {
                "ExpectedDepartureTime": "2025-10-16T10:35:00Z"
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder =
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        method.invoke(generator, call, builder, Long.MIN_VALUE);

        assertTrue(builder.hasDeparture());
        assertTrue(builder.getDeparture().getTime() > 0);
    }

    @Test
    void testSetDepartureTimeWithAimedDepartureTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setDepartureTime",
            JsonNode.class,
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class,
            long.class);
        method.setAccessible(true);

        String json = """
            {
                "AimedDepartureTime": "2025-10-16T10:35:00Z"
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder =
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        method.invoke(generator, call, builder, Long.MIN_VALUE);

        assertTrue(builder.hasDeparture());
    }

    @Test
    void testSetDepartureTimeWithNoDepartureTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "setDepartureTime",
            JsonNode.class,
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder.class,
            long.class);
        method.setAccessible(true);

        String json = "{}";

        JsonNode call = objectMapper.readTree(json);
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder =
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        method.invoke(generator, call, builder, Long.MIN_VALUE);

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
        trips.add(new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip1", "route1", 0, 3600, 0, "20231122"));
        trips.add(new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip2", "route1", 1, 7200, 0, "20231122"));
        trips.add(new org.jouca.idfm_gtfs_rt.finders.TripFinder.TripMeta("trip3", "route2", 0, 5400, 0, "20231122"));
        
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

    // =========================================================================
    // Tests pour la résolution du quai (ExpectedQuayRef)
    // =========================================================================

    @Test
    void testResolveQuayStopIdWithDepartureStopAssignment() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveQuayStopId", JsonNode.class);
        method.setAccessible(true);

        // JSON exact reproduisant le cas utilisateur : quai STIF:StopPoint:Q:472796:
        String json = """
            {
                "StopPointRef": {
                    "value": "STIF:StopArea:SP:58718:"
                },
                "DepartureStopAssignment": {
                    "ExpectedQuayRef": {
                        "value": "STIF:StopPoint:Q:472796:"
                    }
                },
                "ArrivalStopAssignment": {
                    "ExpectedQuayRef": {
                        "value": "STIF:StopPoint:Q:472796:"
                    }
                }
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, call);

        assertEquals("IDFM:472796", result,
            "Le quai devrait être résolu en IDFM:472796 depuis DepartureStopAssignment");
    }

    @Test
    void testResolveQuayStopIdWithOnlyArrivalStopAssignment() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveQuayStopId", JsonNode.class);
        method.setAccessible(true);

        String json = """
            {
                "ArrivalStopAssignment": {
                    "ExpectedQuayRef": {
                        "value": "STIF:StopPoint:Q:471581:"
                    }
                }
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, call);

        assertEquals("IDFM:471581", result,
            "Le quai devrait être résolu en IDFM:471581 depuis ArrivalStopAssignment");
    }

    @Test
    void testResolveQuayStopIdWithNoAssignment() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveQuayStopId", JsonNode.class);
        method.setAccessible(true);

        // Pas de DepartureStopAssignment ni ArrivalStopAssignment → null
        String json = """
            {
                "StopPointRef": {
                    "value": "STIF:StopArea:SP:58718:"
                }
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, call);

        assertNull(result, "Aucun quai ne devrait être résolu si aucun StopAssignment n'est présent");
    }

    @Test
    void testResolveQuayStopIdWithNonNumericQuayId() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveQuayStopId", JsonNode.class);
        method.setAccessible(true);

        String json = """
            {
                "DepartureStopAssignment": {
                    "ExpectedQuayRef": {
                        "value": "STIF:StopPoint:Q:ABCDEF:"
                    }
                }
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, call);

        assertNull(result, "Un quai non numérique ne devrait pas être résolu");
    }

    @Test
    void testResolveQuayStopIdWithMissingExpectedQuayRef() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "resolveQuayStopId", JsonNode.class);
        method.setAccessible(true);

        // DepartureStopAssignment présent mais sans ExpectedQuayRef
        String json = """
            {
                "DepartureStopAssignment": {
                    "AimedQuayRef": {
                        "value": "STIF:StopPoint:Q:472796:"
                    }
                }
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        String result = (String) method.invoke(generator, call);

        assertNull(result, "Sans ExpectedQuayRef, aucun quai ne devrait être résolu");
    }

    @Test
    void testHasSkippedStatusWithOnTime() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "hasSkippedStatus", JsonNode.class);
        method.setAccessible(true);

        // Le JSON utilisateur a DepartureStatus: ON_TIME et ArrivalStatus: ON_TIME
        String json = """
            {
                "DepartureStatus": "ON_TIME",
                "ArrivalStatus": "ON_TIME"
            }
            """;

        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);

        assertFalse(result,
            "ON_TIME ne doit pas être traité comme SKIPPED — le quai ne doit pas être éliminé pour cette raison");
    }

    @Test
    void testIsEstimatedCallInPastWhenArrivalPastButDepartureFuture() throws Exception {
        // Reproduit le cas où le train est à quai : arrivée passée, départ futur.
        // isEstimatedCallInPast renvoie true dès que l'arrivée est passée,
        // ce qui entraîne la suppression du STU et l'absence d'assignation de quai.
        java.lang.reflect.Field epochField = TripUpdateGenerator.class.getDeclaredField("currentEpochSecond");
        epochField.setAccessible(true);
        epochField.setLong(generator, System.currentTimeMillis() / 1000);

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isEstimatedCallInPast", JsonNode.class);
        method.setAccessible(true);

        String pastArrival  = java.time.Instant.now().minusSeconds(60).toString(); // arrivée il y a 1 min
        String futureDeparture = java.time.Instant.now().plusSeconds(120).toString(); // départ dans 2 min

        String json = String.format("""
            {
                "ExpectedArrivalTime": "%s",
                "ExpectedDepartureTime": "%s"
            }
            """, pastArrival, futureDeparture);

        JsonNode call = objectMapper.readTree(json);
        boolean result = (boolean) method.invoke(generator, call);

        // Ce test documente le comportement actuel : true (arrivée passée → tout l'arrêt est ignoré).
        // Conséquence : si un train est à quai (arrivé mais pas encore parti),
        // le STU est supprimé et le quai n'apparaît pas dans le feed.
        assertTrue(result,
            "Comportement actuel : dès que ExpectedArrivalTime est passée, isEstimatedCallInPast=true, "
            + "même si le départ est dans le futur. Cela explique pourquoi le quai disparaît "
            + "pour les trains déjà à quai.");
    }

    // --- Disruption stop-closure -> SKIPPED merging (markStopSkipped / indexTripUpdatesByTripId / isWithinAnyWindow) ---

    private java.lang.reflect.Method markStopSkippedMethod() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markStopSkipped", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, String.class, String.class, String.class, String.class, int.class);
        method.setAccessible(true);
        return method;
    }

    @Test
    void testMarkStopSkippedCreatesMinimalTripUpdateWhenNoneExists() throws Exception {
        // The recommended behaviour for disruption-derived closures: even a trip with zero
        // live SIRI-Lite data should get a minimal TripUpdate carrying the SKIPPED stop, so the
        // closure is reflected in TripUpdates and not only in the Alerts feed.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        markStopSkippedMethod().invoke(generator, feedMessage, index, "trip1", "IDFM:C01563", "20260901",
            "IDFM:11341", 5);

        assertEquals(1, feedMessage.getEntityCount());
        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntity(0).getTripUpdate();
        assertEquals("trip1", tripUpdate.getTrip().getTripId());
        assertEquals("IDFM:C01563", tripUpdate.getTrip().getRouteId());
        assertEquals("20260901", tripUpdate.getTrip().getStartDate());
        assertEquals(
            com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED,
            tripUpdate.getTrip().getScheduleRelationship());

        assertEquals(1, tripUpdate.getStopTimeUpdateCount());
        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate stu = tripUpdate.getStopTimeUpdate(0);
        assertEquals(5, stu.getStopSequence());
        assertEquals("IDFM:11341", stu.getStopId());
        assertEquals(
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            stu.getScheduleRelationship());
    }

    @Test
    void testMarkStopSkippedInsertsInStopSequenceOrderIntoExistingTripUpdate() throws Exception {
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();

        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder existing = feedMessage.addEntityBuilder()
            .setId("trip1")
            .getTripUpdateBuilder();
        existing.getTripBuilder().setTripId("trip1").setRouteId("IDFM:C01563");
        existing.addStopTimeUpdateBuilder().setStopSequence(1).setStopId("IDFM:11340");
        existing.addStopTimeUpdateBuilder().setStopSequence(10).setStopId("IDFM:11343");

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", existing);

        // Insert a SKIPPED stop between the two existing sequences.
        markStopSkippedMethod().invoke(generator, feedMessage, index, "trip1", "IDFM:C01563", "20260901",
            "IDFM:11341", 5);

        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntityBuilder(0)
            .getTripUpdateBuilder().build();
        assertEquals(3, tripUpdate.getStopTimeUpdateCount());
        assertEquals(1, tripUpdate.getStopTimeUpdate(0).getStopSequence());
        assertEquals(5, tripUpdate.getStopTimeUpdate(1).getStopSequence());
        assertEquals("IDFM:11341", tripUpdate.getStopTimeUpdate(1).getStopId());
        assertEquals(
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            tripUpdate.getStopTimeUpdate(1).getScheduleRelationship());
        assertEquals(10, tripUpdate.getStopTimeUpdate(2).getStopSequence());
    }

    @Test
    void testMarkStopSkippedConvertsExistingScheduledEntryToSkipped() throws Exception {
        // Reproduces the real-world case found via production testing: IDFM's live SIRI-Lite
        // vehicle-monitoring feed keeps predicting a normal arrival/departure at a stop closed
        // for months by construction (e.g. Metro 8 / Republique), because the per-call status
        // has no notion of a planned disruption. If we deferred to that existing entry, the
        // closure would never surface as SKIPPED for any trip with live coverage — which in
        // practice is nearly all of them for a busy line. The planned-disruption signal must win:
        // an existing non-SKIPPED entry at that stop_sequence gets converted in place, and its
        // stale arrival/departure predictions cleared.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();

        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder existing = feedMessage.addEntityBuilder()
            .setId("trip1")
            .getTripUpdateBuilder();
        existing.getTripBuilder().setTripId("trip1").setRouteId("IDFM:C01563");
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(5)
            .setStopId("IDFM:11341")
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
            .setArrival(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120));

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", existing);

        markStopSkippedMethod().invoke(generator, feedMessage, index, "trip1", "IDFM:C01563", "20260901",
            "IDFM:11341", 5);

        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntityBuilder(0)
            .getTripUpdateBuilder().build();
        assertEquals(1, tripUpdate.getStopTimeUpdateCount());
        assertEquals(
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            tripUpdate.getStopTimeUpdate(0).getScheduleRelationship());
        assertFalse(tripUpdate.getStopTimeUpdate(0).hasArrival());
    }

    @Test
    void testMarkStopSkippedLeavesAlreadySkippedEntryAlone() throws Exception {
        // If the stop visit is already SKIPPED (e.g. SIRI-Lite itself reported the vehicle
        // missed/cancelled that call, or an earlier pass already converted it), there is nothing
        // to do — avoid redundant mutation.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();

        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder existing = feedMessage.addEntityBuilder()
            .setId("trip1")
            .getTripUpdateBuilder();
        existing.getTripBuilder().setTripId("trip1").setRouteId("IDFM:C01563");
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(5)
            .setStopId("IDFM:11341")
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", existing);

        markStopSkippedMethod().invoke(generator, feedMessage, index, "trip1", "IDFM:C01563", "20260901",
            "IDFM:11341", 5);

        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntityBuilder(0)
            .getTripUpdateBuilder().build();
        assertEquals(1, tripUpdate.getStopTimeUpdateCount());
        assertEquals(
            com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            tripUpdate.getStopTimeUpdate(0).getScheduleRelationship());
    }

    @Test
    void testMarkStopSkippedFallsBackToStopIdWhenSequenceHasDrifted() throws Exception {
        // Reproduces a real production case (Metro 8 / Republique): a live SIRI-Lite entity had
        // a StopTimeUpdate at the closure's computed stop_sequence (16) whose stop_id was actually
        // a DIFFERENT, neighbouring stop ("Strasbourg - Saint-Denis"), while the real target stop
        // ("Republique") sat one sequence later (17), still SCHEDULED. Trusting stop_sequence alone
        // would have marked the wrong stop as skipped and left the actually-closed stop untouched.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();

        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder existing = feedMessage.addEntityBuilder()
            .setId("trip1")
            .getTripUpdateBuilder();
        existing.getTripBuilder().setTripId("trip1").setRouteId("IDFM:C01378");
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(15)
            .setStopId("IDFM:463115"); // Bonne Nouvelle
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(16)
            .setStopId("IDFM:22159") // Strasbourg - Saint-Denis, mismatched at this sequence
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(17)
            .setStopId("IDFM:462962") // Republique — the actual closure target, still SCHEDULED
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", existing);

        // The closure computes stop_sequence=16 for stop_id=IDFM:462962 from the static GTFS —
        // matching what production observed.
        markStopSkippedMethod().invoke(generator, feedMessage, index, "trip1", "IDFM:C01378", "20260904",
            "IDFM:462962", 16);

        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntityBuilder(0)
            .getTripUpdateBuilder().build();
        assertEquals(3, tripUpdate.getStopTimeUpdateCount(), "must not insert a duplicate entry");

        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate seq16 = tripUpdate.getStopTimeUpdate(1);
        assertEquals("IDFM:22159", seq16.getStopId());
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED,
            seq16.getScheduleRelationship(), "the mismatched neighbour must not be marked skipped");

        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate seq17 = tripUpdate.getStopTimeUpdate(2);
        assertEquals("IDFM:462962", seq17.getStopId());
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            seq17.getScheduleRelationship(), "the actual target stop must be marked skipped");
    }

    @Test
    void testMarkSectionSkippedMarksOnlyStopsStrictlyBetweenBoundaries() throws Exception {
        // Reproduces the real RER B case: "no service between X and Y" must mark every actual
        // interior stop of the trip within that range, regardless of IDFM's separate (and, for a
        // severe disruption like this one, unreliable) per-stop impactedObjects list — stops
        // outside the range, AND the boundary stops themselves, must be untouched. Across many
        // real disruptions on many operators, IDFM's from/to consistently name the two
        // still-served stops the closure is anchored on (see this method's javadoc), not stops
        // that are themselves unserved.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        java.util.List<String> stopTimeRows = java.util.List.of(
            "STOP_BEFORE,100,100,1",
            "FROM_STATION,200,200,2",
            "STOP_MID_1,300,300,3",
            "STOP_MID_2,400,400,4",
            "TO_STATION,500,500,5",
            "STOP_AFTER,600,600,6");

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip1", "IDFM:C01743", 0, 100, 600, "20260906");
        org.jouca.idfm_gtfs_rt.records.StopClosure.Section section =
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Section("FROM_STATION", "TO_STATION");
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1_000_000_000L));

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markSectionSkipped", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, TripFinder.TripMeta.class, java.util.List.class, long.class,
            org.jouca.idfm_gtfs_rt.records.StopClosure.Section.class, java.util.List.class);
        method.setAccessible(true);
        method.invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, section, activePeriods);

        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate =
            feedMessage.getEntityBuilder(0).getTripUpdateBuilder().build();

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship> byStop =
            new java.util.HashMap<>();
        for (com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate stu : tripUpdate.getStopTimeUpdateList()) {
            byStop.put(stu.getStopId(), stu.getScheduleRelationship());
        }

        assertFalse(byStop.containsKey("STOP_BEFORE"), "stop before the section must be untouched");
        assertFalse(byStop.containsKey("STOP_AFTER"), "stop after the section must be untouched");
        assertFalse(byStop.containsKey("FROM_STATION"), "the section's own from-boundary must not be touched");
        assertFalse(byStop.containsKey("TO_STATION"), "the section's own to-boundary must not be touched");
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            byStop.get("STOP_MID_1"));
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            byStop.get("STOP_MID_2"));
    }

    @Test
    void testMarkSectionSkippedNoOpWhenTripDoesNotPassThroughBothBoundaries() throws Exception {
        // A different branch/short-turn trip that doesn't reach one of the boundary stations
        // must not be touched at all.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        java.util.List<String> stopTimeRows = java.util.List.of(
            "STOP_BEFORE,100,100,1",
            "FROM_STATION,200,200,2",
            "STOP_MID_1,300,300,3");
        // TO_STATION is never reached by this short-turn trip.

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip2", "IDFM:C01743", 0, 100, 300, "20260906");
        org.jouca.idfm_gtfs_rt.records.StopClosure.Section section =
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Section("FROM_STATION", "TO_STATION");
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1_000_000_000L));

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markSectionSkipped", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, TripFinder.TripMeta.class, java.util.List.class, long.class,
            org.jouca.idfm_gtfs_rt.records.StopClosure.Section.class, java.util.List.class);
        method.setAccessible(true);
        method.invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, section, activePeriods);

        assertEquals(0, feedMessage.getEntityCount(), "no entity should be created for an unaffected trip pattern");
    }

    @Test
    void testMarkSectionSkippedNoOpWhenTripRunsOppositeDirection() throws Exception {
        // Reproduces the real Bus 366 case: IDFM's disruption message says the closure only
        // applies "en direction de Asnières Bords de Seine" (a specific, single direction) even
        // though the same two boundary stations are also served by buses going the other way.
        // IDFM doesn't expose direction as a structured field, but from/to are ordered to match
        // the affected direction's own travel order — confirmed against the real GTFS data
        // (direction_id 0, "Asnières Bords de Seine": Président Kennedy seq4 -> Solférino seq8;
        // direction_id 1, "Église de Colombes": Solférino seq17 -> Président Kennedy seq22).
        // A trip running the OTHER way (visiting "to" before "from") must not be touched at all.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        // This trip runs Solférino -> ... -> Président Kennedy, i.e. the opposite of the
        // disruption's stated "Président Kennedy -> Solférino" direction.
        java.util.List<String> stopTimeRows = java.util.List.of(
            "TO_STATION,100,100,1",
            "STOP_MID,200,200,2",
            "FROM_STATION,300,300,3");

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip3", "IDFM:C01306", 1, 100, 300, "20260906");
        org.jouca.idfm_gtfs_rt.records.StopClosure.Section section =
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Section("FROM_STATION", "TO_STATION");
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1_000_000_000L));

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markSectionSkipped", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, TripFinder.TripMeta.class, java.util.List.class, long.class,
            org.jouca.idfm_gtfs_rt.records.StopClosure.Section.class, java.util.List.class);
        method.setAccessible(true);
        method.invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, section, activePeriods);

        assertEquals(0, feedMessage.getEntityCount(),
            "a trip running opposite to the section's from->to order must not be affected");
    }

    // --- A section's boundary stops confirmed ON_TIME by live data are never overridden
    // (isConfirmedServedByLiveData) ---

    @Test
    void testMarkSectionSkippedDoesNotOverrideBoundaryStopsConfirmedOnTimeByLiveData() throws Exception {
        // Reproduces the real Ligne N case: a disruption titled "Clamart non desservie" whose
        // structured section boundary was Meudon -> Vanves-Malakoff — the two stations either
        // side of the one actually-closed station, not themselves affected. Live SIRI-Lite data
        // for every real train confirmed Meudon and Vanves-Malakoff ON_TIME throughout. Treating
        // the section as inclusive of its boundary must not override that live confirmation —
        // only the interior stop with no live data of its own should get SKIPPED.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();

        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder existing =
            feedMessage.addEntityBuilder().setId("trip1").getTripUpdateBuilder();
        existing.getTripBuilder().setTripId("trip1")
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED);
        // FROM_STATION (Meudon) and TO_STATION (Vanves-Malakoff) are already live-confirmed
        // ON_TIME; MID_STATION (Clamart) has no live entry at all.
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(2)
            .setStopId("FROM_STATION")
            .getDepartureBuilder().setTime(500L);
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(4)
            .setStopId("TO_STATION")
            .getDepartureBuilder().setTime(700L);

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", existing);

        java.util.List<String> stopTimeRows = java.util.List.of(
            "STOP_BEFORE,100,100,1",
            "FROM_STATION,200,200,2",
            "MID_STATION,300,300,3",
            "TO_STATION,400,400,4",
            "STOP_AFTER,500,500,5");

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip1", "IDFM:C01736", 0, 100, 500, "20260906");
        org.jouca.idfm_gtfs_rt.records.StopClosure.Section section =
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Section("FROM_STATION", "TO_STATION");
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1_000_000_000L));

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markSectionSkipped", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, TripFinder.TripMeta.class, java.util.List.class, long.class,
            org.jouca.idfm_gtfs_rt.records.StopClosure.Section.class, java.util.List.class);
        method.setAccessible(true);
        method.invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, section, activePeriods);

        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate =
            feedMessage.getEntityBuilder(0).getTripUpdateBuilder().build();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate> byStop =
            new java.util.HashMap<>();
        for (com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate stu : tripUpdate.getStopTimeUpdateList()) {
            byStop.put(stu.getStopId(), stu);
        }

        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED,
            byStop.get("FROM_STATION").getScheduleRelationship(),
            "a boundary stop already confirmed ON_TIME by live data must not be overridden");
        assertEquals(500L, byStop.get("FROM_STATION").getDeparture().getTime(), "its live time must be untouched");
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED,
            byStop.get("TO_STATION").getScheduleRelationship(),
            "the other boundary stop confirmed ON_TIME by live data must not be overridden");
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            byStop.get("MID_STATION").getScheduleRelationship(),
            "the genuinely closed interior stop, with no live data of its own, must still be skipped");
    }

    @Test
    void testIsConfirmedServedByLiveDataFalseWhenNoLiveEntryExists() throws Exception {
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isConfirmedServedByLiveData", java.util.Map.class, String.class, String.class, int.class);
        method.setAccessible(true);

        assertFalse((boolean) method.invoke(generator, index, "trip1", "STOP_A", 2));
    }

    @Test
    void testIsConfirmedServedByLiveDataFalseWhenLiveEntryIsAlreadySkipped() throws Exception {
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder tripUpdate =
            feedMessage.addEntityBuilder().setId("trip1").getTripUpdateBuilder();
        tripUpdate.addStopTimeUpdateBuilder()
            .setStopSequence(2)
            .setStopId("STOP_A")
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", tripUpdate);

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isConfirmedServedByLiveData", java.util.Map.class, String.class, String.class, int.class);
        method.setAccessible(true);

        assertFalse((boolean) method.invoke(generator, index, "trip1", "STOP_A", 2),
            "an already-SKIPPED live entry is not a confirmation of normal service");
    }

    @Test
    void testMarkSectionSkippedStillAppliesToAlreadyPastStopsWithNoLiveData() throws Exception {
        // A stop already in the past has no live SIRI-Lite call left to confirm it either way
        // (calls drop off the list once visited), but that must not make it default to looking
        // "normal": a journey-detail view showing the whole trip (past and future stops
        // together) would otherwise show an already-passed stop as served fine even though it
        // was, in reality, just as closed as the upcoming stops of the very same closure. Absent
        // any live signal (past or future), the static section is still the best information
        // available and must be applied consistently regardless of "now" — this must hold
        // regardless of the value of currentEpochSecond (a past-stop guard was tried and reverted;
        // this pins down that it must not resurface).
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        java.util.List<String> stopTimeRows = java.util.List.of(
            "FROM_STATION,200,200,1",
            "MID_STATION,300,300,2",
            "TO_STATION,400,400,3");

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip1", "IDFM:C01736", 0, 200, 400, "20260906");
        org.jouca.idfm_gtfs_rt.records.StopClosure.Section section =
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Section("FROM_STATION", "TO_STATION");
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1_000_000_000L));

        java.lang.reflect.Field currentEpochField = TripUpdateGenerator.class.getDeclaredField("currentEpochSecond");
        currentEpochField.setAccessible(true);
        currentEpochField.set(generator, 350L); // "now" is between MID_STATION (300) and TO_STATION (400)

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markSectionSkipped", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, TripFinder.TripMeta.class, java.util.List.class, long.class,
            org.jouca.idfm_gtfs_rt.records.StopClosure.Section.class, java.util.List.class);
        method.setAccessible(true);
        method.invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, section, activePeriods);

        // MID_STATION (300, already before "now" = 350) must still be marked SKIPPED; the
        // boundary stops FROM_STATION/TO_STATION are excluded regardless (see the section-
        // exclusivity tests) and so must not appear at all, past or not.
        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate =
            feedMessage.getEntityBuilder(0).getTripUpdateBuilder().build();
        assertEquals(1, tripUpdate.getStopTimeUpdateCount());
        assertEquals("MID_STATION", tripUpdate.getStopTimeUpdate(0).getStopId());
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED,
            tripUpdate.getStopTimeUpdate(0).getScheduleRelationship(),
            "MID_STATION must be SKIPPED regardless of already being in the past");
    }

    // --- Live SIRI-Lite times take priority over a stale static schedule (preferLiveEpoch) ---

    @Test
    void testMarkSectionSkippedTrustsLiveOnTimeStatusOverStaleStaticSchedule() throws Exception {
        // Reproduces a real case on Ligne R: the static GTFS schedule for a trip lags behind
        // IDFM's actual current timetable (a schedule change not yet reflected in the last
        // imported static feed), so its static departure time (here: within the closure window)
        // disagrees with what SIRI-Lite already reports live for that same stop visit (ON_TIME,
        // well after the window). Trusting the static time alone would wrongly cancel a train
        // that SIRI-Lite already confirms is running fine. Uses an interior stop (not a section
        // boundary) so this specifically exercises preferLiveEpoch's time comparison, independent
        // of the separate boundary-exclusion rule covered by the section-exclusivity tests.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();

        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder existing =
            feedMessage.addEntityBuilder().setId("trip1").getTripUpdateBuilder();
        existing.getTripBuilder().setTripId("trip1")
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED);
        // Live SIRI-Lite already reports MID_STATION as departing well after the closure window
        // (2_000_000L), i.e. this specific stop visit is not actually affected by the closure.
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(3)
            .setStopId("MID_STATION")
            .getDepartureBuilder().setTime(2_000_000L);

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", existing);

        java.util.List<String> stopTimeRows = java.util.List.of(
            "STOP_BEFORE,100,100,1",
            "FROM_STATION,200,200,2",
            "MID_STATION,300,300,3",
            "TO_STATION,400,400,4");

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip1", "IDFM:C01743", 0, 100, 400, "20260906");
        org.jouca.idfm_gtfs_rt.records.StopClosure.Section section =
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Section("FROM_STATION", "TO_STATION");
        // The closure window covers the STATIC schedule's times (0-1000) but not the live time.
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1000L));

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markSectionSkipped", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, TripFinder.TripMeta.class, java.util.List.class, long.class,
            org.jouca.idfm_gtfs_rt.records.StopClosure.Section.class, java.util.List.class);
        method.setAccessible(true);
        method.invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, section, activePeriods);

        com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate updated =
            existing.getStopTimeUpdate(0);
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED,
            updated.getScheduleRelationship(),
            "the live ON_TIME status must win over the stale static schedule's window match");
        assertEquals(2_000_000L, updated.getDeparture().getTime(), "the live time must be left untouched");
    }

    @Test
    void testPreferLiveEpochFallsBackToStaticWhenNoLiveEntryExists() throws Exception {
        // The core case this whole mechanism exists for: no SIRI-Lite data at all for this trip
        // (e.g. its vehicle isn't referenced because the trip is fully interrupted), so there is
        // nothing live to prefer — the static schedule time must be used as-is.
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "preferLiveEpoch", java.util.Map.class, String.class, String.class, int.class, long.class);
        method.setAccessible(true);

        long result = (long) method.invoke(generator, index, "trip1", "STOP_A", 2, 12345L);
        assertEquals(12345L, result);
    }

    @Test
    void testPreferLiveEpochIgnoresAlreadySkippedLiveEntry() throws Exception {
        // A live entry that is itself already SKIPPED (e.g. by ordinary SIRI-Lite cancellation
        // processing) carries no useful arrival/departure time — the static time must be used.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder tripUpdate =
            feedMessage.addEntityBuilder().setId("trip1").getTripUpdateBuilder();
        tripUpdate.addStopTimeUpdateBuilder()
            .setStopSequence(2)
            .setStopId("STOP_A")
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", tripUpdate);

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "preferLiveEpoch", java.util.Map.class, String.class, String.class, int.class, long.class);
        method.setAccessible(true);

        long result = (long) method.invoke(generator, index, "trip1", "STOP_A", 2, 12345L);
        assertEquals(12345L, result, "an already-SKIPPED live entry has no time to prefer");
    }

    // --- Entire-route closure -> whole-trip CANCELED (markTripCanceledIfWithinWindow / markTripCanceled) ---

    private java.lang.reflect.Method markTripCanceledIfWithinWindowMethod() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "markTripCanceledIfWithinWindow", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class,
            java.util.Map.class, TripFinder.TripMeta.class, java.util.List.class, long.class, java.util.List.class);
        method.setAccessible(true);
        return method;
    }

    @Test
    void testMarkTripCanceledIfWithinWindowCancelsWholeTripWhenOverlapping() throws Exception {
        // A whole-line closure (e.g. maintenance work) with no specific stops/sections named:
        // every trip whose schedule overlaps the closure window must be canceled outright.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        java.util.List<String> stopTimeRows = java.util.List.of(
            "STOP_A,100,100,1",
            "STOP_B,200,200,2",
            "STOP_C,300,300,3");

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip1", "IDFM:C01376", 0, 100, 300, "20260906");
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1_000_000_000L));

        markTripCanceledIfWithinWindowMethod().invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, activePeriods);

        assertEquals(1, feedMessage.getEntityCount());
        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate =
            feedMessage.getEntityBuilder(0).getTripUpdateBuilder().build();
        assertEquals("trip1", tripUpdate.getTrip().getTripId());
        assertEquals("IDFM:C01376", tripUpdate.getTrip().getRouteId());
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED,
            tripUpdate.getTrip().getScheduleRelationship());
        assertEquals(0, tripUpdate.getStopTimeUpdateCount(),
            "a canceled trip must not carry stop_time_updates");
    }

    @Test
    void testMarkTripCanceledIfWithinWindowNoOpWhenTripOutsideWindow() throws Exception {
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();

        java.util.List<String> stopTimeRows = java.util.List.of(
            "STOP_A,100,100,1",
            "STOP_B,200,200,2");

        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip1", "IDFM:C01376", 0, 100, 200, "20260906");
        // The closure window ends well before this trip's schedule even starts.
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(-1000L, -500L));

        markTripCanceledIfWithinWindowMethod().invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, activePeriods);

        assertEquals(0, feedMessage.getEntityCount(), "a trip outside every closure window must be untouched");
    }

    @Test
    void testMarkTripCanceledIfWithinWindowClearsExistingStopTimeUpdates() throws Exception {
        // A trip that already has a live SIRI-Lite TripUpdate (with normal stop_time_updates)
        // must have them cleared once the whole trip is canceled by the closure — a canceled
        // trip carrying arrival/departure predictions would be self-contradictory.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder existing =
            feedMessage.addEntityBuilder().setId("trip1").getTripUpdateBuilder();
        existing.getTripBuilder().setTripId("trip1")
            .setScheduleRelationship(com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED);
        existing.addStopTimeUpdateBuilder()
            .setStopSequence(1)
            .setStopId("STOP_A")
            .getArrivalBuilder().setTime(100L);

        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> index = new java.util.HashMap<>();
        index.put("trip1", existing);

        java.util.List<String> stopTimeRows = java.util.List.of("STOP_A,100,100,1");
        TripFinder.TripMeta trip = new TripFinder.TripMeta("trip1", "IDFM:C01376", 0, 100, 100, "20260906");
        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> activePeriods = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(0L, 1_000_000_000L));

        markTripCanceledIfWithinWindowMethod().invoke(generator, feedMessage, index, trip, stopTimeRows, 0L, activePeriods);

        com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate =
            feedMessage.getEntityBuilder(0).getTripUpdateBuilder().build();
        assertEquals(com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED,
            tripUpdate.getTrip().getScheduleRelationship());
        assertEquals(0, tripUpdate.getStopTimeUpdateCount());
    }

    @Test
    void testIndexTripUpdatesByTripId() throws Exception {
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();
        feedMessage.addEntityBuilder().setId("a").getTripUpdateBuilder().getTripBuilder().setTripId("tripA");
        feedMessage.addEntityBuilder().setId("b").getTripUpdateBuilder().getTripBuilder().setTripId("tripB");
        // A non-TripUpdate entity (alert) must be ignored rather than throwing.
        feedMessage.addEntityBuilder().setId("c").getAlertBuilder();

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "indexTripUpdatesByTripId", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder> result =
            (java.util.Map<String, com.google.transit.realtime.GtfsRealtime.TripUpdate.Builder>) method.invoke(generator, feedMessage);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("tripA"));
        assertTrue(result.containsKey("tripB"));
    }

    @Test
    void testIsWithinAnyWindow() throws Exception {
        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "isWithinAnyWindow", long.class, java.util.List.class);
        method.setAccessible(true);

        java.util.List<org.jouca.idfm_gtfs_rt.records.StopClosure.Window> windows = java.util.List.of(
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(1000L, 2000L),
            new org.jouca.idfm_gtfs_rt.records.StopClosure.Window(5000L, 6000L));

        assertTrue((boolean) method.invoke(generator, 1500L, windows));
        assertTrue((boolean) method.invoke(generator, 1000L, windows));
        assertTrue((boolean) method.invoke(generator, 6000L, windows));
        assertFalse((boolean) method.invoke(generator, 3000L, windows));
        assertFalse((boolean) method.invoke(generator, 999L, windows));
    }

    @Test
    void testApplyDisruptionStopClosuresIsNoOpWhenAlertGeneratorNotWired() throws Exception {
        // In this environment (and in these unit tests), TripUpdateGenerator is constructed
        // directly rather than through Spring, so the @Autowired(required=false) AlertGenerator
        // stays null. The disruption-closure pass must degrade to a no-op rather than throwing.
        com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder feedMessage =
            com.google.transit.realtime.GtfsRealtime.FeedMessage.newBuilder();

        java.lang.reflect.Method method = TripUpdateGenerator.class.getDeclaredMethod(
            "applyDisruptionStopClosures", com.google.transit.realtime.GtfsRealtime.FeedMessage.Builder.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(generator, feedMessage));
        assertEquals(0, feedMessage.getEntityCount());
    }
}
