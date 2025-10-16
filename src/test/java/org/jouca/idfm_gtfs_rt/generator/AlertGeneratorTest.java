package org.jouca.idfm_gtfs_rt.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
