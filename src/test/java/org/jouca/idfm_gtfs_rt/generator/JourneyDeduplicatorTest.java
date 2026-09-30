package org.jouca.idfm_gtfs_rt.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class JourneyDeduplicatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** calls: {stopCode, aimedIso} pairs */
    private static JsonNode journey(String ref, String line, String dest, String recordedAt, String... stopAndAimed) {
        StringBuilder calls = new StringBuilder();
        for (int i = 0; i < stopAndAimed.length; i += 2) {
            if (i > 0) {
                calls.append(',');
            }
            calls.append("{\"StopPointRef\":{\"value\":\"STIF:StopPoint:Q:").append(stopAndAimed[i])
                    .append(":\"},\"AimedDepartureTime\":\"").append(stopAndAimed[i + 1]).append("\"}");
        }
        try {
            return MAPPER.readTree("{\"RecordedAtTime\":\"" + recordedAt + "\","
                    + "\"LineRef\":{\"value\":\"STIF:Line::" + line + ":\"},"
                    + "\"DatedVehicleJourneyRef\":{\"value\":\"" + ref + "\"},"
                    + "\"DestinationRef\":{\"value\":\"STIF:StopPoint:Q:" + dest + ":\"},"
                    + "\"EstimatedCalls\":{\"EstimatedCall\":[" + calls + "]}}");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void mirroredProducersPublishingSameCourseAreMerged() {
        JsonNode a = journey("506KRPFO:VehicleJourney::1859979:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:10:00Z", "2", "2026-09-30T05:15:00Z", "9", "2026-09-30T05:20:00Z");
        // mirror published later, already past the first stop: fewer calls, same aimed times
        JsonNode b = journey("506KRPFOV4:VehicleJourney::2013424932093:LOC", "C1", "9", "2026-09-30T05:00:40Z",
                "2", "2026-09-30T05:15:00Z", "9", "2026-09-30T05:20:00Z");

        JourneyDeduplicator.Result result = JourneyDeduplicator.mergeMirroredJourneys(List.of(a, b));

        assertEquals(1, result.dropped());
        assertEquals(1, result.kept().size());
        assertSame(b, result.kept().get(0), "the freshest publication is kept");
    }

    @Test
    void twoVehiclesOfTheSameProducerAreNeverMerged() {
        JsonNode a = journey("MELUN:VehicleJourney::11010:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:10:00Z", "9", "2026-09-30T05:20:00Z");
        JsonNode b = journey("MELUN:VehicleJourney::11009:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:10:00Z", "9", "2026-09-30T05:20:00Z");

        JourneyDeduplicator.Result result = JourneyDeduplicator.mergeMirroredJourneys(List.of(a, b));

        assertEquals(0, result.dropped());
        assertEquals(2, result.kept().size());
    }

    @Test
    void mirroredProducersWithConflictingAimedTimesAreDistinctCourses() {
        JsonNode a = journey("506KRPFO:VehicleJourney::1:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:10:00Z", "9", "2026-09-30T05:20:00Z");
        JsonNode b = journey("506KRPFOV4:VehicleJourney::2:LOC", "C1", "9", "2026-09-30T05:00:40Z",
                "1", "2026-09-30T05:40:00Z", "9", "2026-09-30T05:50:00Z");

        assertEquals(0, JourneyDeduplicator.mergeMirroredJourneys(List.of(a, b)).dropped());
    }

    @Test
    void mirroredProducersWithNothingInCommonAreKept() {
        JsonNode a = journey("506KRPFO:VehicleJourney::1:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:10:00Z");
        JsonNode b = journey("506KRPFOV4:VehicleJourney::2:LOC", "C1", "9", "2026-09-30T05:00:40Z",
                "2", "2026-09-30T05:10:00Z");

        assertEquals(0, JourneyDeduplicator.mergeMirroredJourneys(List.of(a, b)).dropped());
    }

    @Test
    void differentLineOrDestinationIsNotMerged() {
        JsonNode a = journey("506KRPFO:VehicleJourney::1:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:10:00Z");
        JsonNode otherLine = journey("506KRPFOV4:VehicleJourney::2:LOC", "C2", "9", "2026-09-30T05:00:40Z",
                "1", "2026-09-30T05:10:00Z");
        JsonNode otherDest = journey("506KRPFOV4:VehicleJourney::3:LOC", "C1", "8", "2026-09-30T05:00:40Z",
                "1", "2026-09-30T05:10:00Z");

        assertEquals(0, JourneyDeduplicator.mergeMirroredJourneys(List.of(a, otherLine, otherDest)).dropped());
    }

    @Test
    void oneMirrorPublishesTwoCoursesOnlyTheMatchingOneIsDropped() {
        // KRPFO has courses X and Y; KRPFOV4 mirrors both. X and Y overlap on one stop but differ in time.
        JsonNode x = journey("506KRPFO:VehicleJourney::X:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:10:00Z", "9", "2026-09-30T05:20:00Z");
        JsonNode y = journey("506KRPFO:VehicleJourney::Y:LOC", "C1", "9", "2026-09-30T05:00:10Z",
                "1", "2026-09-30T05:30:00Z", "9", "2026-09-30T05:40:00Z");
        JsonNode xMirror = journey("506KRPFOV4:VehicleJourney::XM:LOC", "C1", "9", "2026-09-30T05:00:50Z",
                "1", "2026-09-30T05:10:00Z", "9", "2026-09-30T05:20:00Z");
        JsonNode yMirror = journey("506KRPFOV4:VehicleJourney::YM:LOC", "C1", "9", "2026-09-30T05:00:50Z",
                "1", "2026-09-30T05:30:00Z", "9", "2026-09-30T05:40:00Z");

        JourneyDeduplicator.Result result = JourneyDeduplicator.mergeMirroredJourneys(List.of(x, y, xMirror, yMirror));

        assertEquals(2, result.dropped());
        assertTrue(result.kept().contains(xMirror) && result.kept().contains(yMirror));
    }

    @Test
    void journeysWithoutAimedTimesAreLeftAlone() {
        JsonNode a = journey("RATP-SIV:VehicleJourney::1:LOC", "C1", "9", "2026-09-30T05:00:10Z");
        JsonNode b = journey("RATP-SIV:VehicleJourney::2:LOC", "C1", "9", "2026-09-30T05:00:10Z");

        assertEquals(0, JourneyDeduplicator.mergeMirroredJourneys(List.of(a, b)).dropped());
    }

    @Test
    void mirrorWithASingleCallLeftNeverReplacesOneThatCanBeMatched() {
        // vehicle almost at the terminus: the fresher mirror only has its last call left
        JsonNode stale = journey("506KRPFO:VehicleJourney::1859022:LOC", "C1", "9", "2026-09-30T05:35:22Z",
                "1", "2026-09-30T04:49:20Z", "2", "2026-09-30T04:52:00Z", "9", "2026-09-30T04:55:22Z");
        JsonNode fresh = journey("506KRPFOV4:VehicleJourney::6713424933343:LOC", "C1", "9", "2026-09-30T05:43:44Z",
                "9", "2026-09-30T04:55:22Z");

        JourneyDeduplicator.Result result = JourneyDeduplicator.mergeMirroredJourneys(List.of(stale, fresh));

        assertEquals(1, result.dropped());
        assertSame(stale, result.kept().get(0));
    }

    @Test
    void aWinnerIsNeverDroppedByAStalerJourneyOverlappingItsMirror() {
        // twin timetables: the stale short KRPFO journey overlaps the V4 journey too, but the V4
        // journey already took the long KRPFO journey as its mirror
        JsonNode longMirror = journey("506KRPFO:VehicleJourney::1864603:LOC", "C1", "9", "2026-09-30T05:43:53Z",
                "1", "2026-09-30T05:51:00Z", "2", "2026-09-30T05:52:00Z", "3", "2026-09-30T05:55:12Z");
        JsonNode v4 = journey("506KRPFOV4:VehicleJourney::25213424931751:LOC", "C1", "9", "2026-09-30T05:46:07Z",
                "1", "2026-09-30T05:51:00Z", "2", "2026-09-30T05:52:00Z", "3", "2026-09-30T05:55:12Z");
        JsonNode shortTwin = journey("506KRPFO:VehicleJourney::1864607:LOC", "C1", "9", "2026-09-30T03:58:09Z",
                "1", "2026-09-30T05:51:00Z", "2", "2026-09-30T05:52:00Z");

        JourneyDeduplicator.Result result = JourneyDeduplicator.mergeMirroredJourneys(List.of(longMirror, v4, shortTwin));

        assertEquals(1, result.dropped());
        assertSame(v4, result.kept().stream().filter(n -> n == v4).findFirst().orElse(null));
        assertEquals(2, result.kept().size());
    }
}
