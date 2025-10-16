package org.jouca.idfm_gtfs_rt.generator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TripUpdateGenerator.
 */
class TripUpdateGeneratorTest {

    @BeforeEach
    void setUp() {
        // Clear trip states before each test
        TripUpdateGenerator.tripStates.clear();
        TripUpdateGenerator.vehicleToTrip.clear();
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
}
