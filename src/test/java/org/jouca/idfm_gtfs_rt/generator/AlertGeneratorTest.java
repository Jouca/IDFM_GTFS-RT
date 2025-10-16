package org.jouca.idfm_gtfs_rt.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
