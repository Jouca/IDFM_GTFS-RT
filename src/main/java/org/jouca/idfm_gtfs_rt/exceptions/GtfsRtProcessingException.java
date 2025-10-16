package org.jouca.idfm_gtfs_rt.exceptions;

/**
 * Exception thrown when GTFS-RT feed generation fails during data processing.
 * 
 * <p>This exception indicates that an error occurred while processing SIRI Lite
 * data into GTFS-RT format. It may wrap underlying causes such as:
 * <ul>
 *   <li>Parallel processing failures</li>
 *   <li>Data validation errors</li>
 *   <li>Thread interruptions</li>
 *   <li>Unexpected data format issues</li>
 * </ul>
 * 
 * @author Jouca
 * @since 1.0
 */
public class GtfsRtProcessingException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new GTFS-RT processing exception with the specified detail message.
     * 
     * @param message the detail message explaining the error
     */
    public GtfsRtProcessingException(String message) {
        super(message);
    }

    /**
     * Constructs a new GTFS-RT processing exception with the specified detail message and cause.
     * 
     * @param message the detail message explaining the error
     * @param cause the underlying cause of the error
     */
    public GtfsRtProcessingException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new GTFS-RT processing exception with the specified cause.
     * 
     * @param cause the underlying cause of the error
     */
    public GtfsRtProcessingException(Throwable cause) {
        super(cause);
    }
}
