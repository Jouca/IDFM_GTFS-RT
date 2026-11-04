package org.jouca.idfm_gtfs_rt.generator;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.transit.realtime.GtfsRealtime;

import org.jouca.idfm_gtfs_rt.fetchers.CachedFetch;
import org.jouca.idfm_gtfs_rt.fetchers.ElevatorEnricher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Appends elevator outage alerts to an existing GTFS-RT {@link GtfsRealtime.FeedMessage.Builder}.
 *
 * <p>Elevators are grouped by station ({@code zdcid}). One {@code Alert} entity is emitted
 * per affected station with a user-friendly summary:
 * <ul>
 *   <li>"Tous les ascenseurs sont hors service" when every elevator in the station is down</li>
 *   <li>"X ascenseur(s) sur Y hors service" for partial outages</li>
 * </ul>
 *
 * <p>Alert fields:
 * <ul>
 *   <li>{@code effect = ACCESSIBILITY_ISSUE}</li>
 *   <li>{@code cause  = TECHNICAL_PROBLEM}</li>
 *   <li>{@code severity_level = WARNING}</li>
 *   <li>{@code informed_entity.stop_id = "IDFM:{zdcid}"} (parent station)</li>
 * </ul>
 */
@Component
public class ElevatorAlertGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ElevatorAlertGenerator.class);

    /**
     * The elevator status is polled by every alerts cycle but only changes every few minutes: it is cached, and
     * the last value keeps being served for up to an hour if the open-data API is temporarily down (rather than
     * dropping every elevator alert for the duration of the outage).
     */
    private final CachedFetch<List<JsonNode>> elevatorStatus = new CachedFetch<>(
            ElevatorEnricher::fetchElevators, Duration.ofMinutes(3), Duration.ofHours(1), Clock.systemUTC());

    /**
     * Fetches real-time elevator status and appends one {@code ACCESSIBILITY_ISSUE}
     * alert per affected station to {@code feed}.
     *
     * @param feed the feed builder to append to
     */
    public void addElevatorAlertsToFeed(GtfsRealtime.FeedMessage.Builder feed) {
        List<JsonNode> elevators;
        try {
            elevators = elevatorStatus.get();
        } catch (Exception e) {
            logger.warn("Could not fetch elevator status (non-critical): {}", e.getMessage());
            return;
        }

        // Group all elevators by zdcid, tracking total count and down list
        record StationInfo(String zdcName, List<JsonNode> all, List<JsonNode> down) {}
        Map<String, StationInfo> byStation = new LinkedHashMap<>();

        for (JsonNode elevator : elevators) {
            String zdcId   = elevator.path("zdcid").asText(null);
            String zdcName = elevator.path("zdcname").asText("Gare inconnue");
            if (zdcId == null || zdcId.isBlank()) continue;

            StationInfo info = byStation.computeIfAbsent(zdcId,
                k -> new StationInfo(zdcName, new ArrayList<>(), new ArrayList<>()));
            info.all().add(elevator);
            if ("notavailable".equals(elevator.path("liftstatus").asText(null))) {
                info.down().add(elevator);
            }
        }

        int alertCount = 0;
        for (Map.Entry<String, StationInfo> entry : byStation.entrySet()) {
            String zdcId      = entry.getKey();
            StationInfo info  = entry.getValue();
            if (info.down().isEmpty()) continue;

            int total = info.all().size();
            int down  = info.down().size();
            boolean allDown = (down == total);

            String header = allDown
                ? "Tous les ascenseurs sont hors service — " + info.zdcName()
                : (down == 1
                    ? "1 ascenseur hors service — " + info.zdcName()
                    : down + " ascenseurs sur " + total + " hors service — " + info.zdcName());

            String description = allDown
                ? "Tous les ascenseurs de cette station sont actuellement hors service."
                : (down == 1
                    ? "1 ascenseur sur " + total + " est actuellement hors service dans cette station."
                    : down + " ascenseurs sur " + total + " sont actuellement hors service dans cette station.");

            feed.addEntityBuilder()
                .setId("elevator_station_" + zdcId)
                .getAlertBuilder()
                .addActivePeriod(GtfsRealtime.TimeRange.newBuilder()
                    .setStart(System.currentTimeMillis() / 1000L))
                .addInformedEntityBuilder()
                    .setStopId("IDFM:" + zdcId).build();

            // Re-fetch builder to set remaining fields (addEntityBuilder returns the alert builder directly)
            int lastIdx = feed.getEntityCount() - 1;
            GtfsRealtime.Alert.Builder alert = feed.getEntityBuilder(lastIdx).getAlertBuilder();
            alert.setCause(GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM);
            alert.setEffect(GtfsRealtime.Alert.Effect.ACCESSIBILITY_ISSUE);
            alert.setSeverityLevel(GtfsRealtime.Alert.SeverityLevel.WARNING);
            alert.setHeaderText(GtfsRealtime.TranslatedString.newBuilder()
                .addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder()
                    .setLanguage("fr")
                    .setText(header)));
            alert.setDescriptionText(GtfsRealtime.TranslatedString.newBuilder()
                .addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder()
                    .setLanguage("fr")
                    .setText(description)));

            alertCount++;
        }

        logger.info("Added {} elevator outage alert(s) ({} stations affected)", alertCount,
            byStation.values().stream().filter(i -> !i.down().isEmpty()).count());
    }
}
