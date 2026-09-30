package org.jouca.idfm_gtfs_rt.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

import com.google.transit.realtime.GtfsRealtime;

class ElapsedStopPrunerTest {

    private static final long NOW = 1_000_000L;

    private static GtfsRealtime.FeedEntity.Builder entity() {
        GtfsRealtime.FeedEntity.Builder e = GtfsRealtime.FeedEntity.newBuilder().setId("T1");
        e.getTripUpdateBuilder().getTripBuilder().setTripId("T1");
        return e;
    }

    private static void stop(GtfsRealtime.FeedEntity.Builder e, int seq, long arrival, long departure) {
        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder s = e.getTripUpdateBuilder().addStopTimeUpdateBuilder()
                .setStopSequence(seq).setStopId("S" + seq);
        if (arrival > 0) {
            s.getArrivalBuilder().setTime(arrival);
        }
        if (departure > 0) {
            s.getDepartureBuilder().setTime(departure);
        }
    }

    @Test
    void keepsOnlyUpcomingStops() {
        GtfsRealtime.FeedEntity.Builder e = entity();
        stop(e, 1, NOW - 900, NOW - 880);
        stop(e, 2, NOW - 10, NOW - 5);
        stop(e, 3, NOW + 60, NOW + 80);

        GtfsRealtime.FeedEntity pruned = ElapsedStopPruner.prune(e.build(), NOW);

        assertEquals(1, pruned.getTripUpdate().getStopTimeUpdateCount());
        assertEquals(3, pruned.getTripUpdate().getStopTimeUpdate(0).getStopSequence());
    }

    @Test
    void aStopStillBeingDepartedFromIsKept() {
        GtfsRealtime.FeedEntity.Builder e = entity();
        stop(e, 1, NOW - 30, NOW + 30);

        GtfsRealtime.FeedEntity built = e.build();
        assertSame(built, ElapsedStopPruner.prune(built, NOW));
    }

    @Test
    void tripWithOnlyElapsedStopsIsDropped() {
        GtfsRealtime.FeedEntity.Builder e = entity();
        stop(e, 1, NOW - 900, NOW - 880);
        stop(e, 2, NOW - 100, NOW - 90);

        assertNull(ElapsedStopPruner.prune(e.build(), NOW));
    }

    @Test
    void scheduledTripWithoutAnyStopIsDropped() {
        assertNull(ElapsedStopPruner.prune(entity().build(), NOW));
    }

    @Test
    void canceledTripWithoutStopsIsKept() {
        GtfsRealtime.FeedEntity.Builder e = entity();
        e.getTripUpdateBuilder().getTripBuilder()
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);

        GtfsRealtime.FeedEntity built = e.build();
        assertSame(built, ElapsedStopPruner.prune(built, NOW));
    }

    @Test
    void skippedStopsAreKeptEvenWhenElapsed() {
        GtfsRealtime.FeedEntity.Builder e = entity();
        e.getTripUpdateBuilder().addStopTimeUpdateBuilder().setStopSequence(1).setStopId("S1")
                .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
        stop(e, 2, NOW - 900, NOW - 880);
        stop(e, 3, NOW + 60, NOW + 80);

        GtfsRealtime.FeedEntity pruned = ElapsedStopPruner.prune(e.build(), NOW);

        assertEquals(2, pruned.getTripUpdate().getStopTimeUpdateCount());
        assertEquals(1, pruned.getTripUpdate().getStopTimeUpdate(0).getStopSequence());
        assertEquals(3, pruned.getTripUpdate().getStopTimeUpdate(1).getStopSequence());
    }

    @Test
    void stopsWithoutAnyTimeAreKept() {
        GtfsRealtime.FeedEntity.Builder e = entity();
        stop(e, 1, 0, 0);

        GtfsRealtime.FeedEntity built = e.build();
        assertSame(built, ElapsedStopPruner.prune(built, NOW));
    }
}
