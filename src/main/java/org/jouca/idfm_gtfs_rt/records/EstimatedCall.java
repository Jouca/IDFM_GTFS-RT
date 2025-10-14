package org.jouca.idfm_gtfs_rt.records;

/**
 * Represents an estimated call at a transit stop.
 * <p>
 * This record encapsulates information about a vehicle's estimated arrival or departure
 * at a specific stop, including the stop identifier and the estimated time in ISO 8601 format.
 * It is typically used in real-time transit updates to provide passengers with accurate
 * arrival and departure predictions.
 * </p>
 *
 * @param stopId  The unique identifier of the transit stop. This should correspond to a stop_id
 *                in the GTFS static data.
 * @param isoTime The estimated arrival or departure time in ISO 8601 format (e.g., "2025-10-14T15:30:00Z").
 *                This represents the predicted time when a vehicle will reach or leave the stop.
 *
 * @author Jouca
 * @since 1.0
 */
public record EstimatedCall(String stopId, String isoTime) {}