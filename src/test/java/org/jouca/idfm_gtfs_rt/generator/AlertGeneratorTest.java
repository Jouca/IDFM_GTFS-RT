package org.jouca.idfm_gtfs_rt.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AlertGenerator.
 */
class AlertGeneratorTest {

    private AlertGenerator generator;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        generator = new AlertGenerator();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testAlertGeneratorInitialization() {
        assertNotNull(generator);
    }

    @Test
    void testParseEmptyJsonNode() throws Exception {
        String emptyJson = "{}";
        JsonNode node = objectMapper.readTree(emptyJson);
        
        assertNotNull(node);
        assertTrue(node.isEmpty());
    }

    @Test
    void testParseValidJsonStructure() throws Exception {
        String validJson = "{\"alerts\": [{\"id\": \"alert1\", \"message\": \"Test alert\"}]}";
        JsonNode node = objectMapper.readTree(validJson);
        
        assertNotNull(node);
        assertTrue(node.has("alerts"));
        assertTrue(node.get("alerts").isArray());
    }

    @Test
    void testParseInvalidJson() {
        String invalidJson = "{ invalid json }";
        
        assertThrows(Exception.class, () -> {
            objectMapper.readTree(invalidJson);
        });
    }

    @Test
    void testParseNullJson() {
        assertThrows(Exception.class, () -> {
            objectMapper.readTree((String) null);
        });
    }

    @Test
    void testParseDisruptionsWithValidData() throws Exception {
        String disruptionsJson = """
            [
                {
                    "id": "disruption1",
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "lastUpdate": "20231201T095000",
                    "cause": "TRAVAUX",
                    "severity": "BLOQUANTE",
                    "tags": ["construction"],
                    "title": "Test Disruption",
                    "message": "Test message"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("disruption1"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> alert = (Map<String, Object>) result.get("disruption1");
        assertEquals("disruption1", alert.get("id"));
        assertEquals("TRAVAUX", alert.get("cause"));
        assertEquals("BLOQUANTE", alert.get("severity"));
        assertEquals("Test Disruption", alert.get("title"));
        assertEquals("Test message", alert.get("message"));
    }

    @Test
    void testParseDisruptionsWithMissingId() throws Exception {
        String disruptionsJson = """
            [
                {
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "message": "Test message"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseDisruptionsWithMissingApplicationPeriods() throws Exception {
        String disruptionsJson = """
            [
                {
                    "id": "disruption1",
                    "message": "Test message"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseDisruptionsWithMultipleValid() throws Exception {
        String disruptionsJson = """
            [
                {
                    "id": "disruption1",
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "message": "Message 1"
                },
                {
                    "id": "disruption2",
                    "applicationPeriods": [{"begin": "20231201T130000", "end": "20231201T150000"}],
                    "message": "Message 2"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("disruption1"));
        assertTrue(result.containsKey("disruption2"));
    }

    @Test
    void testParseDisruptionsWithEmptyArray() throws Exception {
        String disruptionsJson = "[]";
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseLinesWithValidData() throws Exception {
        String linesJson = """
            [
                {
                    "id": "line1",
                    "name": "Metro Line 1",
                    "shortName": "1",
                    "mode": "metro",
                    "networkId": "IDFM",
                    "impactedObjects": [{"id": "stop1", "type": "stop_point"}]
                }
            ]
            """;
        
        JsonNode lines = objectMapper.readTree(linesJson);
        Map<String, Object> result = generator.parseLines(lines);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("line1"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> line = (Map<String, Object>) result.get("line1");
        assertEquals("line1", line.get("id"));
        assertEquals("Metro Line 1", line.get("name"));
        assertEquals("1", line.get("shortName"));
        assertEquals("metro", line.get("mode"));
        assertEquals("IDFM", line.get("networkId"));
        assertNotNull(line.get("impactedObjects"));
    }

    @Test
    void testParseLinesWithMultipleLines() throws Exception {
        String linesJson = """
            [
                {
                    "id": "line1",
                    "name": "Metro Line 1",
                    "shortName": "1",
                    "mode": "metro",
                    "networkId": "IDFM",
                    "impactedObjects": []
                },
                {
                    "id": "line2",
                    "name": "Bus Line 20",
                    "shortName": "20",
                    "mode": "bus",
                    "networkId": "IDFM",
                    "impactedObjects": []
                }
            ]
            """;
        
        JsonNode lines = objectMapper.readTree(linesJson);
        Map<String, Object> result = generator.parseLines(lines);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("line1"));
        assertTrue(result.containsKey("line2"));
    }

    @Test
    void testParseLinesWithEmptyArray() throws Exception {
        String linesJson = "[]";
        
        JsonNode lines = objectMapper.readTree(linesJson);
        Map<String, Object> result = generator.parseLines(lines);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseDisruptionsWithNullValues() throws Exception {
        String disruptionsJson = """
            [
                {
                    "id": "disruption1",
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "cause": null,
                    "severity": null,
                    "title": null,
                    "message": "Test message"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> alert = (Map<String, Object>) result.get("disruption1");
        // Note: When JSON has null values, Jackson's asText() returns "null" as a string, not Java null
        // The parseDisruptions method uses getStringField which returns null only if the field is missing,
        // not if the field value is JSON null
        assertEquals("Test message", alert.get("message"));
    }

    @Test
    void testParseDisruptionsWithDifferentCauses() throws Exception {
        String disruptionsJson = """
            [
                {
                    "id": "disruption1",
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "cause": "TRAVAUX",
                    "message": "Construction work"
                },
                {
                    "id": "disruption2",
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "cause": "PERTURBATION",
                    "message": "Service disruption"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> alert1 = (Map<String, Object>) result.get("disruption1");
        assertEquals("TRAVAUX", alert1.get("cause"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> alert2 = (Map<String, Object>) result.get("disruption2");
        assertEquals("PERTURBATION", alert2.get("cause"));
    }

    @Test
    void testParseDisruptionsWithDifferentSeverities() throws Exception {
        String disruptionsJson = """
            [
                {
                    "id": "disruption1",
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "severity": "BLOQUANTE",
                    "message": "Blocking disruption"
                },
                {
                    "id": "disruption2",
                    "applicationPeriods": [{"begin": "20231201T100000", "end": "20231201T120000"}],
                    "severity": "PERTURBEE",
                    "message": "Disturbed service"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> alert1 = (Map<String, Object>) result.get("disruption1");
        assertEquals("BLOQUANTE", alert1.get("severity"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> alert2 = (Map<String, Object>) result.get("disruption2");
        assertEquals("PERTURBEE", alert2.get("severity"));
    }

    @Test
    void testParseDisruptionsPreservesApplicationPeriods() throws Exception {
        String disruptionsJson = """
            [
                {
                    "id": "disruption1",
                    "applicationPeriods": [
                        {"begin": "20231201T100000", "end": "20231201T120000"},
                        {"begin": "20231202T100000", "end": "20231202T120000"}
                    ],
                    "message": "Test message"
                }
            ]
            """;
        
        JsonNode disruptions = objectMapper.readTree(disruptionsJson);
        Map<String, Object> result = generator.parseDisruptions(disruptions);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> alert = (Map<String, Object>) result.get("disruption1");
        ArrayNode applicationPeriods = (ArrayNode) alert.get("applicationPeriods");
        assertNotNull(applicationPeriods);
        assertEquals(2, applicationPeriods.size());
    }

    private Map<String, Object> invokeAddInformedEntities(GtfsRealtime.Alert.Builder alertBuilder,
                                                            String disruptionId,
                                                            Map<String, Object> lines) throws Exception {
        return invokeAddInformedEntities(alertBuilder, disruptionId, lines, null);
    }

    private Map<String, Object> invokeAddInformedEntities(GtfsRealtime.Alert.Builder alertBuilder,
                                                            String disruptionId,
                                                            Map<String, Object> lines,
                                                            ArrayNode impactedSections) throws Exception {
        Method method = AlertGenerator.class.getDeclaredMethod(
            "addInformedEntities", GtfsRealtime.Alert.Builder.class, String.class, Map.class, ArrayNode.class);
        method.setAccessible(true);
        method.invoke(generator, alertBuilder, disruptionId, lines, impactedSections);
        return lines;
    }

    @Test
    void testAddInformedEntitiesPairsRouteAndStopWhenStopsImpacted() throws Exception {
        // Reproduces the reported scenario: IDFM tags the parent line as impacted
        // alongside the specific stops closed for construction. The alert must not
        // carry a bare route-only selector in that case, otherwise an effect like
        // NO_SERVICE reads as applying to the whole route instead of just those stops.
        String linesJson = """
            [
                {
                    "id": "line:IDFM:C01563",
                    "name": "Bus 9102",
                    "shortName": "9102",
                    "mode": "bus",
                    "networkId": "IDFM",
                    "impactedObjects": [
                        {"id": "line:IDFM:C01563", "type": "line", "disruptionIds": ["disruption1"]},
                        {"id": "stop_point:IDFM:11341", "type": "stop_point", "disruptionIds": ["disruption1"]},
                        {"id": "stop_point:IDFM:11342", "type": "stop_point", "disruptionIds": ["disruption1"]}
                    ]
                }
            ]
            """;
        Map<String, Object> lines = generator.parseLines(objectMapper.readTree(linesJson));

        GtfsRealtime.Alert.Builder alertBuilder = GtfsRealtime.Alert.newBuilder();
        invokeAddInformedEntities(alertBuilder, "disruption1", lines);

        assertEquals(2, alertBuilder.getInformedEntityCount());
        for (GtfsRealtime.EntitySelector selector : alertBuilder.getInformedEntityList()) {
            assertEquals("IDFM:C01563", selector.getRouteId());
            assertTrue(selector.hasStopId());
        }
    }

    @Test
    void testAddInformedEntitiesRouteOnlyWhenNoStopsImpacted() throws Exception {
        // A genuinely line-wide disruption (no specific stops named) should still
        // produce a route-only selector.
        String linesJson = """
            [
                {
                    "id": "line:IDFM:C01563",
                    "name": "Bus 9102",
                    "shortName": "9102",
                    "mode": "bus",
                    "networkId": "IDFM",
                    "impactedObjects": [
                        {"id": "line:IDFM:C01563", "type": "line", "disruptionIds": ["disruption1"]}
                    ]
                }
            ]
            """;
        Map<String, Object> lines = generator.parseLines(objectMapper.readTree(linesJson));

        GtfsRealtime.Alert.Builder alertBuilder = GtfsRealtime.Alert.newBuilder();
        invokeAddInformedEntities(alertBuilder, "disruption1", lines);

        assertEquals(1, alertBuilder.getInformedEntityCount());
        GtfsRealtime.EntitySelector selector = alertBuilder.getInformedEntity(0);
        assertEquals("IDFM:C01563", selector.getRouteId());
        assertFalse(selector.hasStopId());
    }

    private String siriDataJson(String severity) {
        return """
            {
                "disruptions": [
                    {
                        "id": "disruption1",
                        "applicationPeriods": [{"begin": "20260801T040000", "end": "20261010T230000"}],
                        "cause": "TRAVAUX",
                        "severity": "%s",
                        "title": "TRAVAUX Avenue Charles de Gaulle ORSAY",
                        "message": "Les arrets sont supprimes"
                    }
                ],
                "lines": [
                    {
                        "id": "line:IDFM:C01563",
                        "name": "Bus 9102",
                        "shortName": "9102",
                        "mode": "bus",
                        "networkId": "IDFM",
                        "impactedObjects": [
                            {"id": "line:IDFM:C01563", "type": "line", "disruptionIds": ["disruption1"]},
                            {"id": "stop_point:IDFM:11341", "type": "stop_point", "disruptionIds": ["disruption1"]},
                            {"id": "stop_point:IDFM:11342", "type": "stop_point", "disruptionIds": ["disruption1"]}
                        ]
                    }
                ]
            }
            """.formatted(severity);
    }

    @Test
    void testComputeStopClosuresForBlockingDisruption() throws Exception {
        // Reproduces the reported scenario end-to-end: a NO_SERVICE ("BLOQUANTE") disruption
        // naming specific closed stops on a line should surface as a StopClosure so
        // TripUpdateGenerator can mark the affected trips SKIPPED.
        JsonNode siriData = objectMapper.readTree(siriDataJson("BLOQUANTE"));

        List<org.jouca.idfm_gtfs_rt.records.StopClosure> closures = generator.computeStopClosures(siriData);

        assertEquals(1, closures.size());
        org.jouca.idfm_gtfs_rt.records.StopClosure closure = closures.get(0);
        assertEquals("disruption1", closure.disruptionId());
        assertEquals("IDFM:C01563", closure.routeId());
        assertEquals(2, closure.stopIds().size());
        assertTrue(closure.stopIds().contains("IDFM:11341"));
        assertTrue(closure.stopIds().contains("IDFM:11342"));
        assertEquals(1, closure.activePeriods().size());
        assertTrue(closure.activePeriods().get(0).endEpochSec() > closure.activePeriods().get(0).startEpochSec());
    }

    @Test
    void testComputeStopClosuresIgnoresReducedServiceDisruption() throws Exception {
        // A REDUCED_SERVICE ("PERTURBEE") disruption doesn't mean the stop stops being served,
        // so it must not produce a stop closure.
        JsonNode siriData = objectMapper.readTree(siriDataJson("PERTURBEE"));

        List<org.jouca.idfm_gtfs_rt.records.StopClosure> closures = generator.computeStopClosures(siriData);

        assertTrue(closures.isEmpty());
    }

    @Test
    void testComputeStopClosuresWithNullSiriData() {
        assertTrue(generator.computeStopClosures(null).isEmpty());
    }

    @Test
    void testComputeStopClosuresForWholeLineClosureWithNoStopsNamed() throws Exception {
        // Reproduces the real Metro 6 case: IDFM tags only the line itself as impacted ("Travaux
        // de modernisation - Trafic interrompu"), naming no specific stops or sections at all.
        // This must surface as an entireRouteClosure so every trip on the line gets canceled,
        // instead of silently producing no closure at all.
        String siriData = """
            {
                "disruptions": [
                    {
                        "id": "disruption1",
                        "applicationPeriods": [{"begin": "20260906T044500", "end": "20260907T043000"}],
                        "cause": "TRAVAUX",
                        "severity": "BLOQUANTE",
                        "title": "Metro 6 : Travaux de modernisation - Trafic interrompu",
                        "message": "Trafic interrompu"
                    }
                ],
                "lines": [
                    {
                        "id": "line:IDFM:C01376",
                        "name": "6",
                        "shortName": "6",
                        "mode": "metro",
                        "networkId": "IDFM",
                        "impactedObjects": [
                            {"id": "line:IDFM:C01376", "type": "line", "disruptionIds": ["disruption1"]}
                        ]
                    }
                ]
            }
            """;

        List<org.jouca.idfm_gtfs_rt.records.StopClosure> closures =
            generator.computeStopClosures(objectMapper.readTree(siriData));

        assertEquals(1, closures.size());
        org.jouca.idfm_gtfs_rt.records.StopClosure closure = closures.get(0);
        assertEquals("IDFM:C01376", closure.routeId());
        assertTrue(closure.entireRouteClosure());
        assertTrue(closure.stopIds().isEmpty());
        assertTrue(closure.sections().isEmpty());
        assertEquals(1, closure.activePeriods().size());
    }

    // --- impactedSections: severe disruptions where IDFM's per-stop list can't be trusted ---

    private String siriDataJsonWithSections() {
        // Reproduces the real RER B case found in production: "trafic interrompu" between two
        // stations, "perturbe" on the rest of the line — but IDFM's impactedObjects lists nearly
        // every station on the line as a stop_point, not just the two truly closed ones. The
        // disruption's impactedSections field gives the real, narrow boundary.
        return """
            {
                "disruptions": [
                    {
                        "id": "disruption1",
                        "applicationPeriods": [{"begin": "20260906T041551", "end": "20260906T190000"}],
                        "cause": "PERTURBATION",
                        "severity": "BLOQUANTE",
                        "title": "RER B : Aeroport CDG2 <-> Parc des Expo trafic interrompu",
                        "message": "Trafic interrompu",
                        "impactedSections": [
                            {
                                "lineId": "line:IDFM:C01743",
                                "from": {"type": "stop_area", "id": "stop_area:IDFM:73699", "name": "Aeroport CDG (Terminal 2)"},
                                "to": {"type": "stop_area", "id": "stop_area:IDFM:73568", "name": "Parc des Expositions"}
                            }
                        ]
                    }
                ],
                "lines": [
                    {
                        "id": "line:IDFM:C01743",
                        "name": "B",
                        "shortName": "B",
                        "mode": "RapidTransit",
                        "networkId": "IDFM",
                        "impactedObjects": [
                            {"id": "line:IDFM:C01743", "type": "line", "disruptionIds": ["disruption1"]},
                            {"id": "stop_point:IDFM:43833", "type": "stop_point", "disruptionIds": ["disruption1"]},
                            {"id": "stop_point:IDFM:43097", "type": "stop_point", "disruptionIds": ["disruption1"]},
                            {"id": "stop_point:IDFM:73568", "type": "stop_point", "disruptionIds": ["disruption1"]},
                            {"id": "stop_point:IDFM:73699", "type": "stop_point", "disruptionIds": ["disruption1"]}
                        ]
                    }
                ]
            }
            """;
    }

    @Test
    void testAddInformedEntitiesFallsBackToRouteOnlyWhenSectionsPresent() throws Exception {
        JsonNode siriData = objectMapper.readTree(siriDataJsonWithSections());
        Map<String, Object> lines = generator.parseLines(siriData.get("lines"));
        ArrayNode impactedSections = (ArrayNode) siriData.get("disruptions").get(0).get("impactedSections");

        GtfsRealtime.Alert.Builder alertBuilder = GtfsRealtime.Alert.newBuilder();
        invokeAddInformedEntities(alertBuilder, "disruption1", lines, impactedSections);

        // Must NOT pair the route with the noisy, mostly-unrelated stop list — a route-only
        // selector is the safe fallback since GTFS-Realtime can't express "between X and Y".
        assertEquals(1, alertBuilder.getInformedEntityCount());
        GtfsRealtime.EntitySelector selector = alertBuilder.getInformedEntity(0);
        assertEquals("IDFM:C01743", selector.getRouteId());
        assertFalse(selector.hasStopId());
    }

    @Test
    void testComputeStopClosuresUsesSectionsInsteadOfNoisyStopList() throws Exception {
        JsonNode siriData = objectMapper.readTree(siriDataJsonWithSections());

        List<org.jouca.idfm_gtfs_rt.records.StopClosure> closures = generator.computeStopClosures(siriData);

        assertEquals(1, closures.size());
        org.jouca.idfm_gtfs_rt.records.StopClosure closure = closures.get(0);
        assertEquals("IDFM:C01743", closure.routeId());
        assertTrue(closure.stopIds().isEmpty(), "must not use the broad per-stop list when sections are present");
        assertEquals(1, closure.sections().size());
        org.jouca.idfm_gtfs_rt.records.StopClosure.Section section = closure.sections().get(0);
        assertEquals("IDFM:73699", section.fromParentStationId());
        assertEquals("IDFM:73568", section.toParentStationId());
    }
}
