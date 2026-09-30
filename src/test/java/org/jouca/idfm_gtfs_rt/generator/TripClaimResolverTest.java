package org.jouca.idfm_gtfs_rt.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jouca.idfm_gtfs_rt.finders.TripFinder.TripFit;
import org.jouca.idfm_gtfs_rt.generator.TripClaimResolver.Claim;
import org.jouca.idfm_gtfs_rt.generator.TripClaimResolver.Outcome;
import org.junit.jupiter.api.Test;

class TripClaimResolverTest {

    private static Claim claim(String vehicle, String trip, long diffSeconds) {
        return new Claim(vehicle, trip, new TripFit(5, 5, diffSeconds), false, false);
    }

    private static Claim claim(String vehicle, String trip, long diffSeconds, boolean authoritative, boolean cached) {
        return new Claim(vehicle, trip, new TripFit(5, 5, diffSeconds), authoritative, cached);
    }

    private static Set<String> tripsOf(Outcome outcome) {
        Set<String> trips = new HashSet<>();
        outcome.assigned().values().forEach(c -> assertTrue(trips.add(c.tripId()), "trip attributed twice: " + c.tripId()));
        return trips;
    }

    @Test
    void nonConflictingClaimsAreUntouched() {
        Outcome outcome = TripClaimResolver.resolve(List.of(claim("A", "T1", 0), claim("B", "T2", 30)),
                (v, ex) -> { throw new AssertionError("no rematch expected"); });

        assertEquals(Map.of("A", "T1", "B", "T2"), Map.of("A", outcome.assigned().get("A").tripId(),
                "B", outcome.assigned().get("B").tripId()));
        assertTrue(outcome.dropped().isEmpty());
        assertEquals(0, outcome.reassigned());
    }

    @Test
    void twinTripsGetOneVehicleEach() {
        // identical timetables: both vehicles fit T1 equally well, T2 is its twin
        Outcome outcome = TripClaimResolver.resolve(List.of(claim("A", "T1", 0), claim("B", "T1", 0)),
                (v, ex) -> ex.contains("T2") ? null : claim(v, "T2", 0));

        assertEquals(Set.of("T1", "T2"), tripsOf(outcome));
        assertTrue(outcome.dropped().isEmpty());
        assertEquals(1, outcome.reassigned());
    }

    @Test
    void delayedVehicleLosesTheTripItsNeighbourFitsBetter() {
        // B is the 10:10 run delayed by 8 min, so its predicted times fit A's 10:00 trip loosely
        Outcome outcome = TripClaimResolver.resolve(List.of(claim("A", "T1000", 5), claim("B", "T1000", 480)),
                (v, ex) -> claim(v, "T1010", 20));

        assertEquals("T1000", outcome.assigned().get("A").tripId());
        assertEquals("T1010", outcome.assigned().get("B").tripId());
    }

    @Test
    void vehicleWithNoAlternativeIsDroppedInsteadOfDuplicating() {
        Outcome outcome = TripClaimResolver.resolve(List.of(claim("A", "T1", 0), claim("B", "T1", 300)),
                (v, ex) -> null);

        assertEquals(Set.of("T1"), tripsOf(outcome));
        assertEquals("A", outcome.assigned().get("A").vehicleId());
        assertEquals(Set.of("B"), outcome.dropped());
    }

    @Test
    void authoritativeMatchBeatsBetterTimeFit() {
        Outcome outcome = TripClaimResolver.resolve(
                List.of(claim("A", "T1", 0), claim("B", "T1", 600, true, false)), (v, ex) -> null);

        assertTrue(outcome.assigned().containsKey("B"));
        assertFalse(outcome.assigned().containsKey("A"));
    }

    @Test
    void cachedVehicleWinsATie() {
        Outcome outcome = TripClaimResolver.resolve(
                List.of(claim("A", "T1", 10, false, false), claim("B", "T1", 10, false, true)), (v, ex) -> null);

        assertTrue(outcome.assigned().containsKey("B"));
    }

    @Test
    void displacementChainSettles() {
        // A,B fight over T1; B moves to T2 where it beats C; C moves to T3.
        Outcome outcome = TripClaimResolver.resolve(
                List.of(claim("A", "T1", 0), claim("B", "T1", 100), claim("C", "T2", 200)),
                (v, ex) -> switch (v) {
                    case "B" -> claim("B", "T2", 50);
                    case "C" -> claim("C", "T3", 40);
                    default -> null;
                });

        assertEquals(Set.of("T1", "T2", "T3"), tripsOf(outcome));
        assertEquals("T2", outcome.assigned().get("B").tripId());
        assertEquals("T3", outcome.assigned().get("C").tripId());
    }

    @Test
    void neverAttributesATripTwiceEvenWhenRematchKeepsReturningTheSameOne() {
        Outcome outcome = TripClaimResolver.resolve(
                List.of(claim("A", "T1", 0), claim("B", "T1", 10), claim("C", "T1", 20)),
                (v, ex) -> claim(v, "T1", 999));

        assertEquals(Set.of("T1"), tripsOf(outcome));
        assertEquals(2, outcome.dropped().size());
    }

    @Test
    void resultIsIndependentOfInputOrder() {
        List<Claim> forward = List.of(claim("A", "T1", 7), claim("B", "T1", 7), claim("C", "T1", 7));
        List<Claim> backward = List.of(forward.get(2), forward.get(1), forward.get(0));
        TripClaimResolver.Rematcher none = (v, ex) -> null;

        assertEquals(TripClaimResolver.resolve(forward, none).assigned().keySet(),
                TripClaimResolver.resolve(backward, none).assigned().keySet());
    }
}
