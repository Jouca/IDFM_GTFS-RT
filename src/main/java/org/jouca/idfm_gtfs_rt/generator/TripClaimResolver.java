package org.jouca.idfm_gtfs_rt.generator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jouca.idfm_gtfs_rt.finders.TripFinder;

/**
 * Makes sure one GTFS trip is attributed to at most one vehicle per SIRI cycle.
 *
 * <p>Each vehicle is matched to a trip independently, so two vehicles can end up on the same trip:
 * a delayed vehicle whose real times happen to fit its neighbour's schedule, or two vehicles
 * running identical timetables (twin trips). Emitting both would yield several TripUpdates for one
 * trip, and consumers keep an arbitrary one. Here the vehicle whose real-time calls fit the trip
 * best keeps it; the others are re-matched with the taken trips excluded, and dropped when no
 * other trip fits.
 */
final class TripClaimResolver {

    /** Enough rounds for a chain of displaced vehicles to settle; the last leftovers are dropped. */
    static final int MAX_ROUNDS = 8;

    private TripClaimResolver() {
    }

    /**
     * A vehicle's candidate trip.
     *
     * @param authoritative the trip was identified from the vehicle reference itself (not inferred
     *                      from times) and therefore always wins a conflict
     * @param cached        the vehicle already held this trip in a previous cycle (breaks ties in favour of stability)
     */
    record Claim(String vehicleId, String tripId, TripFinder.TripFit fit, boolean authoritative, boolean cached) {
    }

    /** Finds another trip for a vehicle that lost its trip, never returning an excluded one. */
    @FunctionalInterface
    interface Rematcher {
        /** @return a claim on a trip outside {@code excludedTripIds}, or {@code null} if none fits */
        Claim rematch(String vehicleId, Set<String> excludedTripIds);
    }

    /** Final attribution (vehicleId to claim) and the vehicles left without any trip. */
    record Outcome(Map<String, Claim> assigned, Set<String> dropped, int reassigned) {
    }

    static Outcome resolve(List<Claim> initial, Rematcher rematcher) {
        Map<String, Claim> current = new TreeMap<>();
        Map<String, Set<String>> tried = new HashMap<>();
        Set<String> dropped = new HashSet<>();
        Set<String> reassigned = new HashSet<>();
        for (Claim claim : initial) {
            current.put(claim.vehicleId(), claim);
        }

        for (int round = 0; round < MAX_ROUNDS; round++) {
            Map<String, List<Claim>> byTrip = new TreeMap<>();
            for (Claim claim : current.values()) {
                byTrip.computeIfAbsent(claim.tripId(), k -> new ArrayList<>()).add(claim);
            }

            List<Claim> losers = new ArrayList<>();
            for (List<Claim> contenders : byTrip.values()) {
                if (contenders.size() > 1) {
                    contenders.sort(WINNER_FIRST);
                    losers.addAll(contenders.subList(1, contenders.size()));
                }
            }
            if (losers.isEmpty()) {
                break;
            }

            for (Claim loser : losers) {
                Set<String> excluded = tried.computeIfAbsent(loser.vehicleId(), k -> new HashSet<>());
                excluded.add(loser.tripId());
                Claim replacement = rematcher.rematch(loser.vehicleId(), new HashSet<>(excluded));
                if (replacement == null || replacement.tripId() == null) {
                    current.remove(loser.vehicleId());
                    dropped.add(loser.vehicleId());
                } else {
                    current.put(loser.vehicleId(), replacement);
                    reassigned.add(loser.vehicleId());
                }
            }
        }

        // Anything still sharing a trip after the last round loses to the best contender.
        Map<String, Claim> winners = new HashMap<>();
        for (Claim claim : new ArrayList<>(current.values())) {
            Claim holder = winners.get(claim.tripId());
            if (holder == null) {
                winners.put(claim.tripId(), claim);
            } else if (WINNER_FIRST.compare(claim, holder) < 0) {
                current.remove(holder.vehicleId());
                dropped.add(holder.vehicleId());
                winners.put(claim.tripId(), claim);
            } else {
                current.remove(claim.vehicleId());
                dropped.add(claim.vehicleId());
            }
        }

        reassigned.removeAll(dropped);
        return new Outcome(current, dropped, reassigned.size());
    }

    private static final Comparator<Claim> WINNER_FIRST = Comparator
            .comparing((Claim c) -> !c.authoritative())
            .thenComparing((a, b) -> {
                if (a.fit() == null || b.fit() == null) {
                    return Boolean.compare(a.fit() == null, b.fit() == null);
                }
                if (a.fit().betterThan(b.fit())) {
                    return -1;
                }
                return b.fit().betterThan(a.fit()) ? 1 : 0;
            })
            .thenComparing((Claim c) -> !c.cached())
            .thenComparing(Claim::vehicleId);
}
