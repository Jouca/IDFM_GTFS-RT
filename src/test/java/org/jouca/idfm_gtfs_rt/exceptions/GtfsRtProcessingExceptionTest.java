package org.jouca.idfm_gtfs_rt.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GtfsRtProcessingException.
 */
class GtfsRtProcessingExceptionTest {

    @Test
    void testExceptionWithMessage() {
        String message = "Test error message";
        GtfsRtProcessingException exception = new GtfsRtProcessingException(message);
        
        assertEquals(message, exception.getMessage());
    }

    @Test
    void testExceptionWithMessageAndCause() {
        String message = "Test error message";
        Throwable cause = new RuntimeException("Cause exception");
        GtfsRtProcessingException exception = new GtfsRtProcessingException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testExceptionWithNullMessage() {
        GtfsRtProcessingException exception = new GtfsRtProcessingException((String) null);
        assertNull(exception.getMessage());
    }

    @Test
    void testExceptionWithNullCause() {
        String message = "Test error message";
        GtfsRtProcessingException exception = new GtfsRtProcessingException(message, null);
        
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testExceptionIsRuntimeException() {
        GtfsRtProcessingException exception = new GtfsRtProcessingException("Test");
        assertTrue(exception instanceof RuntimeException);
    }
}
