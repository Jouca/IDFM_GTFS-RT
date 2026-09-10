package org.jouca.idfm_gtfs_rt.records;

import java.util.List;

/**
 * Represents a stop-level service closure caused by an IDFM disruption (e.g. construction
 * work closing a couple of stops on a line), as opposed to a whole-route disruption.
 * <p>
 * This is the bridge between the disruption/alert data ingested by {@code AlertGenerator}
 * and the {@code TripUpdateGenerator}, which uses it to mark the affected stop as
 * {@code SKIPPED} on the actual trips that would otherwise have served it, in addition to
 * the general service alert.
 * <p>
 * A closure is described either as an explicit list of closed {@code stopIds} (the common case:
 * IDFM names the specific stops closed by the disruption), as one or more {@code sections}
 * (a "no service between X and Y" boundary, from the disruption's {@code impactedSections}
 * field) when that is the more reliable source — see {@code AlertGenerator.computeRouteImpacts}
 * for why IDFM's per-stop list can't always be trusted on its own for severe disruptions — or, when
 * IDFM names neither specific stops nor a section for a {@code NO_SERVICE} disruption (a whole
 * line closed, e.g. for maintenance work, with no more precise detail given),
 * {@code entireRouteClosure}: every trip on the route is affected, not just some stops on it.
 *
 * @param disruptionId the IDFM disruption identifier this closure originates from
 * @param routeId      the GTFS route id affected
 * @param stopIds      the GTFS stop ids closed on this route for the duration of the disruption
 * @param sections     "no service between two stations" boundaries for this route/disruption
 * @param activePeriods the time windows (Unix epoch seconds) during which the closure applies
 * @param entireRouteClosure whether the whole route has no service (no specific stops/sections
 *                           named), meaning trips overlapping the active periods should be
 *                           canceled entirely rather than having individual stops skipped
 *
 * @author Jouca
 * @since 1.0
 */
public record StopClosure(String disruptionId, String routeId, List<String> stopIds, List<Section> sections,
        List<Window> activePeriods, boolean entireRouteClosure) {

    /**
     * A single active time window, in Unix epoch seconds (inclusive bounds).
     *
     * @param startEpochSec window start
     * @param endEpochSec   window end
     */
    public record Window(long startEpochSec, long endEpochSec) {
        public boolean contains(long epochSec) {
            return epochSec >= startEpochSec && epochSec <= endEpochSec;
        }
    }

    /**
     * A "no service between these two stations" boundary, as given by a disruption's
     * {@code impactedSections} field. The ids are parent station ids (stop_area-level), which
     * must be resolved down to actual stop_ids (see
     * {@code TripFinder#getStopIdsForParentStation}) before they can be matched against a trip's
     * stop_times.
     *
     * @param fromParentStationId one end of the closed segment (order is not significant)
     * @param toParentStationId   the other end of the closed segment
     */
    public record Section(String fromParentStationId, String toParentStationId) {
    }
}
