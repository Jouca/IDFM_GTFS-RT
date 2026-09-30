package org.jouca.idfm_gtfs_rt.generator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Removes SIRI journeys that are the same physical course published twice by two mirrored
 * producers of one operator (for example {@code 506KRPFO} and {@code 506KRPFOV4}).
 *
 * <p>Two journeys are only merged when every signal agrees: same line, same destination, same
 * operator family with different producers, at least one stop in common, and identical aimed
 * times on every common stop. Journeys from the <em>same</em> producer are never merged, because
 * an operator can legitimately run two vehicles on identical timetables; those are handled by
 * {@link TripClaimResolver} instead, which gives each of them its own trip.
 */
final class JourneyDeduplicator {

    /** Aimed times closer than this are considered identical (SIRI rounds to the second/minute). */
    static final long AIMED_TOLERANCE_SECONDS = 60;

    /** TripFinder needs at least two calls to identify a trip. */
    static final int MIN_CALLS_TO_MATCH = 2;

    private static final String FIELD_VALUE = "value";

    private JourneyDeduplicator() {
    }

    /** Outcome of a merge pass: the journeys to keep and how many were dropped as mirrors. */
    record Result(List<JsonNode> kept, int dropped) {
    }

    static Result mergeMirroredJourneys(List<JsonNode> entities) {
        Map<String, List<Journey>> families = new LinkedHashMap<>();
        for (JsonNode entity : entities) {
            Journey journey = Journey.of(entity);
            if (journey != null) {
                families.computeIfAbsent(journey.familyKey(), k -> new ArrayList<>()).add(journey);
            }
        }

        Set<JsonNode> dropped = Collections.newSetFromMap(new IdentityHashMap<>());
        for (List<Journey> family : families.values()) {
            if (family.size() < 2 || family.stream().map(Journey::producer).distinct().count() < 2) {
                continue;
            }
            mergeFamily(family, dropped);
        }

        if (dropped.isEmpty()) {
            return new Result(entities, 0);
        }
        List<JsonNode> kept = new ArrayList<>(entities.size() - dropped.size());
        for (JsonNode entity : entities) {
            if (!dropped.contains(entity)) {
                kept.add(entity);
            }
        }
        return new Result(kept, dropped.size());
    }

    private static void mergeFamily(List<Journey> family, Set<JsonNode> dropped) {
        List<Journey> byFreshness = new ArrayList<>(family);
        // A journey with a single call left cannot be matched to a trip, so it never wins over
        // a mirror that can, even if it was published more recently.
        byFreshness.sort(Comparator.comparing((Journey j) -> j.callCount() < MIN_CALLS_TO_MATCH)
                .thenComparing(Comparator.comparingLong(Journey::recordedAt).reversed())
                .thenComparing(Comparator.comparingInt((Journey j) -> j.callCount()).reversed())
                .thenComparing(Journey::vehicleRef));

        // A journey that already had its turn as winner is settled: a staler journey must not
        // drop it afterwards, otherwise a course could vanish together with its own mirror.
        Set<JsonNode> settled = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Journey winner : byFreshness) {
            if (dropped.contains(winner.entity())) {
                continue;
            }
            settled.add(winner.entity());
            // Take at most one mirror per other producer: a producer publishing two courses
            // that both overlap this one means they are distinct courses, not mirrors.
            Map<String, Journey> bestPerProducer = new HashMap<>();
            Map<String, Integer> bestCommon = new HashMap<>();
            for (Journey other : byFreshness) {
                if (other == winner || dropped.contains(other.entity()) || settled.contains(other.entity()) || other.producer().equals(winner.producer())) {
                    continue;
                }
                int common = winner.commonAimedStops(other);
                if (common > 0 && bestCommon.getOrDefault(other.producer(), 0) < common) {
                    bestCommon.put(other.producer(), common);
                    bestPerProducer.put(other.producer(), other);
                }
            }
            bestPerProducer.values().forEach(mirror -> dropped.add(mirror.entity()));
        }
    }

    /** Immutable digest of the fields the merge rule looks at. */
    private record Journey(JsonNode entity, String vehicleRef, String producer, String family, String lineRef,
            String destinationRef, long recordedAt, int callCount, Map<String, List<Long>> aimedByStop) {

        static Journey of(JsonNode entity) {
            String vehicleRef = text(entity.path("DatedVehicleJourneyRef"));
            String lineRef = text(entity.path("LineRef"));
            String destinationRef = text(entity.path("DestinationRef"));
            if (vehicleRef.isEmpty() || lineRef.isEmpty()) {
                return null;
            }
            String producer = vehicleRef.split(":", 2)[0];
            String family = producer.replaceAll("V\\d+$", "");

            long recordedAt = 0;
            String recorded = entity.path("RecordedAtTime").asText("");
            if (!recorded.isEmpty()) {
                try {
                    recordedAt = Instant.parse(recorded).toEpochMilli();
                } catch (RuntimeException e) {
                    recordedAt = 0;
                }
            }

            Map<String, List<Long>> aimedByStop = new HashMap<>();
            JsonNode calls = entity.path("EstimatedCalls").path("EstimatedCall");
            for (JsonNode call : calls) {
                String stopCode = stopCode(call);
                String aimed = call.hasNonNull("AimedDepartureTime") ? call.get("AimedDepartureTime").asText()
                        : call.path("AimedArrivalTime").asText("");
                if (stopCode == null || aimed.isEmpty()) {
                    continue;
                }
                try {
                    aimedByStop.computeIfAbsent(stopCode, k -> new ArrayList<>())
                            .add(Instant.parse(aimed).getEpochSecond());
                } catch (RuntimeException e) {
                    // unparseable time: this call simply doesn't take part in the comparison
                }
            }
            return new Journey(entity, vehicleRef, producer, family, lineRef, destinationRef, recordedAt,
                    calls.size(), aimedByStop);
        }

        String familyKey() {
            return lineRef + "|" + family + "|" + destinationRef;
        }

        /**
         * Number of stops present in both journeys with matching aimed times, or 0 when there is
         * no common stop or any common stop has conflicting aimed times.
         */
        int commonAimedStops(Journey other) {
            int common = 0;
            for (Map.Entry<String, List<Long>> entry : aimedByStop.entrySet()) {
                List<Long> theirs = other.aimedByStop.get(entry.getKey());
                if (theirs == null) {
                    continue;
                }
                boolean agrees = false;
                for (long mine : entry.getValue()) {
                    for (long their : theirs) {
                        if (Math.abs(mine - their) <= AIMED_TOLERANCE_SECONDS) {
                            agrees = true;
                        }
                    }
                }
                if (!agrees) {
                    return 0;
                }
                common++;
            }
            return common;
        }

        private static String stopCode(JsonNode call) {
            String[] parts = text(call.path("StopPointRef")).split(":");
            return parts.length > 3 ? parts[3] : null;
        }

        private static String text(JsonNode node) {
            if (node.isObject()) {
                return node.path(FIELD_VALUE).asText("");
            }
            return node.asText("");
        }
    }
}
