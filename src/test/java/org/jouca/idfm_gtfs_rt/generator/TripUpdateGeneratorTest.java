package org.jouca.idfm_gtfs_rt.generator;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Test sur la propagation (carry-forward) du dernier offset réel.
 */
public class TripUpdateGeneratorTest {

    // Fabrique un EstimatedCall JSON minimal
    private ObjectNode estimatedCall(ObjectMapper om, String stopCode, String isoTime) {
        ObjectNode call = om.createObjectNode();
        ObjectNode stopPointRef = om.createObjectNode();
        stopPointRef.put("value", "STIF:StopPoint:Q:" + stopCode); // format simplifié
        call.set("StopPointRef", stopPointRef);
        call.put("ExpectedArrivalTime", isoTime);
        return call;
    }

    @Test
    public void testCarryForwardOffsets() throws Exception {
        TripUpdateGenerator gen = new TripUpdateGenerator();
        ObjectMapper om = new ObjectMapper();
        ArrayNode estimatedCalls = om.createArrayNode();

        // Deux points temps réel : seq 2 retard +60s, seq 5 retard +180s
        // Nouvelle logique: seq 3 & 4 héritent de +60s (pas d'interpolation), seq >=5 passe à +180s
        // Pour simplifier: sequences mapping: stop1->1 stop2->2 stop3->3 stop4->4 stop5->5
        // On fabrique une liste scheduledStopTimes correspondant à ces sequences
        List<String> scheduled = new ArrayList<>();
        for (int seq = 1; seq <= 5; seq++) {
            // stop_id, arrival, departure, sequence
            scheduled.add("STOP"+seq+"," + (300*seq) + "," + (300*seq+30) + "," + seq);
        }

        // Temps de base serviceDay = now start of day
    // base non nécessaire, on calcule directement les timestamps théoriques
        // Simuler parseTime => les timestamps réels doivent être base+theoretical+offset.
        // Mais parseTime lit un ISO => on génère des ISO à partir de base + theoretical + offset
        long serviceDayEpoch = Instant.now().atZone(java.time.ZoneId.of("Europe/Paris")).toLocalDate().atStartOfDay(java.time.ZoneId.of("Europe/Paris")).toEpochSecond();
        long theoStop2 = serviceDayEpoch + (300*2);
        long theoStop5 = serviceDayEpoch + (300*5);
        long realStop2 = theoStop2 + 60; // +1 minute
        long realStop5 = theoStop5 + 180; // +3 minutes

        estimatedCalls.add(estimatedCall(om, "STOP2", Instant.ofEpochSecond(realStop2).toString()));
        estimatedCalls.add(estimatedCall(om, "STOP5", Instant.ofEpochSecond(realStop5).toString()));

        // Appel reflectif computeInterpolatedOffsets(List<JsonNode>, List<String>, String)
    Method m = TripUpdateGenerator.class.getDeclaredMethod("computeCarryForwardOffsets", List.class, List.class, String.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<Integer, Long> offsets = (Map<Integer, Long>) m.invoke(gen, estimatedCalls, scheduled, "TRIP1");

        // Vérifications
        assertEquals(60L, offsets.get(2));
        assertEquals(60L, offsets.get(3));
        assertEquals(60L, offsets.get(4));
        assertEquals(180L, offsets.get(5));
    }
}
