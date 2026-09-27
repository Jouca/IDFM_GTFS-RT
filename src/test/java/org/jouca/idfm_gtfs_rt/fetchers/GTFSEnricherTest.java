package org.jouca.idfm_gtfs_rt.fetchers;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GTFSEnricher class.
 */
class GTFSEnricherTest {

    private String invokeEnrichStopsTxt(String stopsTxt, Map<String, GTFSEnricher.QuayData> quayData,
            Map<String, String> arridToZdcid) throws Exception {
        Method method = GTFSEnricher.class.getDeclaredMethod(
            "enrichStopsTxt", InputStream.class, Map.class, Map.class);
        method.setAccessible(true);
        InputStream is = new ByteArrayInputStream(stopsTxt.getBytes(StandardCharsets.UTF_8));
        byte[] result = (byte[]) method.invoke(null, is, quayData, arridToZdcid);
        return new String(result, StandardCharsets.UTF_8);
    }

    private String createdParentStation(String result, String createdStopId) {
        String[] lines = result.split("\n");
        String[] headers = lines[0].split(",");
        int stopIdIdx = -1, parentIdx = -1;
        for (int i = 0; i < headers.length; i++) {
            if ("stop_id".equals(headers[i])) stopIdIdx = i;
            if ("parent_station".equals(headers[i])) parentIdx = i;
        }

        String createdRow = null;
        for (String line : lines) {
            if (line.startsWith(createdStopId + ",")) {
                createdRow = line;
                break;
            }
        }
        assertNotNull(createdRow, "the missing quay must be created");

        String[] fields = createdRow.split(",");
        assertEquals(createdStopId, fields[stopIdIdx]);
        return fields[parentIdx];
    }

    @Test
    void testCreatedStopIsParentedToTheTrueStationNotTheMonomodalStopPlace() throws Exception {
        // Reproduces the real hierarchy reported on Discord (Haussmann Saint-Lazare):
        // IDFM:73688 (location_type=1, the true GTFS Station) is the parent of
        // IDFM:monomodalStopPlace:58718 (location_type=0, a "logical"/representative point for
        // the complex), which is what the NeTEx Quay's ParentZoneRef ("zdcid") identifies.
        // A newly-created quay must NOT be parented directly to that monomodalStopPlace stop --
        // per GTFS, a location_type=0 stop's parent_station must be a location_type=1 Station,
        // and tooling that only walks direct children of the true station (as reported: Bus
        // Tracker) would never find a quay nested two levels down. It must be parented to
        // IDFM:73688 instead, as a proper sibling of the monomodalStopPlace stop.
        String stopsTxt = "stop_id,stop_name,stop_lat,stop_lon,zone_id,location_type,parent_station\n"
            + "IDFM:73688,Haussmann Saint-Lazare,48.875,2.3285,,1,\n"
            + "IDFM:monomodalStopPlace:58718,Haussmann Saint-Lazare,48.875,2.3286,1,0,IDFM:73688\n";

        Map<String, GTFSEnricher.QuayData> quayData = new LinkedHashMap<>();
        quayData.put("471498", new GTFSEnricher.QuayData(
            "Haussmann Saint-Lazare", "48.8757", "2.3244", "1", "58718", "-"));
        Map<String, String> arridToZdcid = new HashMap<>();
        arridToZdcid.put("471498", "58718");

        String result = invokeEnrichStopsTxt(stopsTxt, quayData, arridToZdcid);

        assertEquals("IDFM:73688", createdParentStation(result, "IDFM:471498"),
            "the new quay must be parented to the true location_type=1 station, "
                + "as a sibling of the monomodalStopPlace stop, not a child of it");
    }

    @Test
    void testCreatedStopFallsBackToMonomodalStopPlaceWhenItsOwnParentIsUnknown() throws Exception {
        // If the monomodalStopPlace stop for this zdcid isn't present in this stops.txt at all
        // (so its true station-level parent can't be looked up), fall back to parenting the new
        // quay directly under "IDFM:monomodalStopPlace:{zdcid}" rather than a bare numeric id
        // that matches no stop -- still broken relative to the ideal hierarchy, but never a
        // dangling reference.
        String stopsTxt = "stop_id,stop_name,stop_lat,stop_lon,zone_id,location_type,parent_station\n"
            + "IDFM:99999,Some Other Stop,48.0,2.0,,0,\n";

        Map<String, GTFSEnricher.QuayData> quayData = new LinkedHashMap<>();
        quayData.put("471498", new GTFSEnricher.QuayData(
            "Haussmann Saint-Lazare", "48.8757", "2.3244", "1", "58718", "-"));
        Map<String, String> arridToZdcid = new HashMap<>();
        arridToZdcid.put("471498", "58718");

        String result = invokeEnrichStopsTxt(stopsTxt, quayData, arridToZdcid);

        assertEquals("IDFM:monomodalStopPlace:58718", createdParentStation(result, "IDFM:471498"));
    }
}
