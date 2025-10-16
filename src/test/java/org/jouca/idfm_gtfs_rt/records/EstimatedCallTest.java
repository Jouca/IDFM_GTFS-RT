package org.jouca.idfm_gtfs_rt.records;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EstimatedCall record.
 */
class EstimatedCallTest {

    @Test
    void testEstimatedCallCreation() {
        EstimatedCall call = new EstimatedCall("stop123", "2024-01-15T10:30:00Z");
        
        assertEquals("stop123", call.stopId());
        assertEquals("2024-01-15T10:30:00Z", call.isoTime());
    }

    @Test
    void testEstimatedCallWithNullValues() {
        EstimatedCall call = new EstimatedCall(null, null);
        
        assertNull(call.stopId());
        assertNull(call.isoTime());
    }

    @Test
    void testEstimatedCallEquality() {
        EstimatedCall call1 = new EstimatedCall("stop123", "2024-01-15T10:30:00Z");
        EstimatedCall call2 = new EstimatedCall("stop123", "2024-01-15T10:30:00Z");
        
        assertEquals(call1, call2);
        assertEquals(call1.hashCode(), call2.hashCode());
    }

    @Test
    void testEstimatedCallInequality() {
        EstimatedCall call1 = new EstimatedCall("stop123", "2024-01-15T10:30:00Z");
        EstimatedCall call2 = new EstimatedCall("stop456", "2024-01-15T10:30:00Z");
        
        assertNotEquals(call1, call2);
    }

    @Test
    void testEstimatedCallToString() {
        EstimatedCall call = new EstimatedCall("stop123", "2024-01-15T10:30:00Z");
        String toString = call.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("stop123"));
        assertTrue(toString.contains("2024-01-15T10:30:00Z"));
    }
}
