package org.jouca.idfm_gtfs_rt.generator;

import java.util.ArrayList;
import java.util.List;

import com.google.transit.realtime.GtfsRealtime;

/**
 * Removes stop time updates whose predicted time has already passed.
 *
 * <p>The live-SIRI path already skips elapsed calls, but trips re-emitted from the cache and trips
 * rebuilt from a theoretical schedule carry their whole stop list, past stops included. Consumers
 * have no use for those, and a trip whose stops are all elapsed is finished, so it is dropped
 * instead of being published as an empty (or entirely past) TripUpdate.
 */
final class ElapsedStopPruner {

    private ElapsedStopPruner() {
    }

    /**
     * @return the entity without its elapsed stops, the same instance when nothing changed, or
     *         {@code null} when nothing meaningful is left to publish
     */
    static GtfsRealtime.FeedEntity prune(GtfsRealtime.FeedEntity entity, long nowEpochSeconds) {
        if (!entity.hasTripUpdate()) {
            return entity;
        }
        GtfsRealtime.TripUpdate update = entity.getTripUpdate();
        // A cancelled trip is meaningful without any stop
        if (update.getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
            return entity;
        }

        List<GtfsRealtime.TripUpdate.StopTimeUpdate> remaining = new ArrayList<>(update.getStopTimeUpdateCount());
        for (GtfsRealtime.TripUpdate.StopTimeUpdate stu : update.getStopTimeUpdateList()) {
            if (!isElapsed(stu, nowEpochSeconds)) {
                remaining.add(stu);
            }
        }
        if (remaining.isEmpty()) {
            return null;
        }
        if (remaining.size() == update.getStopTimeUpdateCount()) {
            return entity;
        }
        GtfsRealtime.FeedEntity.Builder builder = entity.toBuilder();
        builder.getTripUpdateBuilder().clearStopTimeUpdate().addAllStopTimeUpdate(remaining);
        return builder.build();
    }

    private static boolean isElapsed(GtfsRealtime.TripUpdate.StopTimeUpdate stu, long now) {
        // SKIPPED stops carry no prediction and their status is worth keeping
        if (stu.getScheduleRelationship() == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED) {
            return false;
        }
        long latest = Math.max(stu.hasArrival() ? stu.getArrival().getTime() : 0,
                stu.hasDeparture() ? stu.getDeparture().getTime() : 0);
        return latest > 0 && latest < now;
    }
}
