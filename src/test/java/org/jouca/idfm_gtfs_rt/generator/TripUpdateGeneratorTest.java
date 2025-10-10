package org.jouca.idfm_gtfs_rt.generator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.jouca.idfm_gtfs_rt.finders.TripFinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.transit.realtime.GtfsRealtime;

/**
 * Test sur la propagation (carry-forward) du dernier offset réel.
 * NOTE: Ce test est désactivé car la fonctionnalité d'ajout des temps théoriques
 * avec offset a été retirée. Le GTFS-RT ne contient maintenant que les données
 * temps réel provenant directement de SIRI Lite.
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
    @Disabled("Fonctionnalité d'ajout des temps théoriques retirée - seules les données temps réel sont conservées")
    public void testCarryForwardOffsets() throws Exception {
        // TripUpdateGenerator gen = new TripUpdateGenerator();
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
    }

    @Test
    public void appendCanceledTripsAddsMissingTheoreticalTrips() throws Exception {
        TripUpdateGenerator generator = new TripUpdateGenerator();
        TripUpdateGenerator.ProcessingContext context = new TripUpdateGenerator.ProcessingContext();

        RouteSample sample = findRouteWithMultipleTrips();
        org.junit.jupiter.api.Assertions.assertNotNull(sample, "Aucune ligne avec deux trajets minimum trouvée dans la base GTFS");

        TripFinder.TripMeta missingTrip = sample.sortedTrips().get(0);
        TripFinder.TripMeta realtimeTrip = sample.sortedTrips().get(1);

        TripUpdateGenerator.RealtimeDirectionStats stats = new TripUpdateGenerator.RealtimeDirectionStats();
        stats.addTrip(realtimeTrip);
        context.statsByRouteDirection
                .computeIfAbsent(sample.routeId(), r -> new java.util.concurrent.ConcurrentHashMap<>())
                .put(sample.directionId(), stats);

        GtfsRealtime.FeedMessage.Builder feedBuilder = GtfsRealtime.FeedMessage.newBuilder();
        feedBuilder.setHeader(GtfsRealtime.FeedHeader.newBuilder()
                .setGtfsRealtimeVersion("2.0")
                .setIncrementality(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET)
                .setTimestamp(System.currentTimeMillis() / 1000L));

        GtfsRealtime.FeedEntity.Builder realtimeEntity = GtfsRealtime.FeedEntity.newBuilder();
        realtimeEntity.setId(realtimeTrip.tripId);
        GtfsRealtime.TripUpdate.Builder realtimeUpdate = realtimeEntity.getTripUpdateBuilder();
        realtimeUpdate.getTripBuilder()
                .setTripId(realtimeTrip.tripId)
                .setRouteId(realtimeTrip.routeId)
                .setDirectionId(realtimeTrip.directionId);
        feedBuilder.addEntity(realtimeEntity.build());

        generator.appendCanceledTrips(feedBuilder, context);

        List<GtfsRealtime.FeedEntity> entities = feedBuilder.getEntityList();
        Optional<GtfsRealtime.FeedEntity> missingEntity = entities.stream()
                .filter(e -> e.getId().equals(missingTrip.tripId))
                .findFirst();

        org.junit.jupiter.api.Assertions.assertTrue(missingEntity.isPresent(), "Le trajet théorique manquant doit être ajouté en tant que suppression");
        org.junit.jupiter.api.Assertions.assertEquals(
                GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED,
                missingEntity.get().getTripUpdate().getTrip().getScheduleRelationship(),
                "Le trajet ajouté doit être marqué comme supprimé");
    }

    private record RouteSample(String routeId, int directionId, List<TripFinder.TripMeta> sortedTrips) {}

    private RouteSample findRouteWithMultipleTrips() throws Exception {
        List<String> routeIds = fetchRouteIds(100);

        for (String routeId : routeIds) {
            List<TripFinder.TripMeta> metas = TripFinder.getActiveTripsForRoutesToday(Collections.singletonList(routeId));
            if (metas == null || metas.size() < 2) {
                continue;
            }

            Map<Integer, List<TripFinder.TripMeta>> byDirection = metas.stream()
                    .collect(Collectors.groupingBy(meta -> meta.directionId));

            for (Map.Entry<Integer, List<TripFinder.TripMeta>> entry : byDirection.entrySet()) {
                List<TripFinder.TripMeta> sorted = entry.getValue().stream()
                        .sorted(Comparator.comparingInt(meta -> meta.firstTimeSecOfDay))
                        .collect(Collectors.toList());
                if (sorted.size() >= 2) {
                    return new RouteSample(routeId, entry.getKey(), sorted);
                }
            }
        }
        return null;
    }

    private List<String> fetchRouteIds(int limit) throws Exception {
        List<String> routes = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:./gtfs.db");
             PreparedStatement stmt = conn.prepareStatement("SELECT route_id FROM routes LIMIT ?")) {
            stmt.setInt(1, limit);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    routes.add(rs.getString("route_id"));
                }
            }
        }
        return routes;
    }
}

