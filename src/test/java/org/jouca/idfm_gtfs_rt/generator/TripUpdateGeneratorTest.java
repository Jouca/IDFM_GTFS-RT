package org.jouca.idfm_gtfs_rt.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @Test
    void testCheckStopIntegrityWithInvalidNonNumericStop() throws Exception {
        String entityJson = """
            {
                "StopPointRef": {
                    "value": "STIF:StopPoint:Q:ABC123"
                }
            }
            """;
        
        JsonNode entity = objectMapper.readTree(entityJson);
        boolean result = generator.checkStopIntegrity(entity);
        
        assertFalse(result);
    }

    @Test
    void testCheckStopIntegrityWithMissingStopPointRef() throws Exception {
        String entityJson = "{}";
        
        JsonNode entity = objectMapper.readTree(entityJson);
        boolean result = generator.checkStopIntegrity(entity);
        
        assertFalse(result);
    }

    @Test
    void testCheckStopIntegrityWithMissingValue() throws Exception {
        String entityJson = """
            {
                "StopPointRef": {}
            }
            """;
        
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
}
