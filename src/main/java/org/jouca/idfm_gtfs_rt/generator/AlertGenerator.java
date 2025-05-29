package org.jouca.idfm_gtfs_rt.generator;

import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.jouca.idfm_gtfs_rt.fetchers.AlertFetcher;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.transit.realtime.GtfsRealtime;

@Component
public class AlertGenerator {
    public void generateAlert() throws Exception {
        System.out.println("Fetching alerts from online data...");
        JsonNode siriData = AlertFetcher.fetchAlertData();

        GtfsRealtime.FeedMessage.Builder feed = GtfsRealtime.FeedMessage.newBuilder();

        Map<String, Object> alertDict = parseDisruptions(siriData.get("disruptions"));
        Map<String, Object> lines = parseLines(siriData.get("lines"));

        // For each alert, create a new alert entity
        for (Map.Entry<String, Object> entry : alertDict.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> alert = (Map<String, Object>) entry.getValue();

            // Set active periods
            for (JsonNode applicationPeriod : (ArrayNode) alert.get("applicationPeriods")) {
                String startStr = applicationPeriod.get("begin").asText();
                String endStr = applicationPeriod.get("end").asText();

                long startEpoch = convertToEpoch(startStr);
                long endEpoch = convertToEpoch(endStr);

                // Create a new entity builder for each application period
                GtfsRealtime.Alert.Builder alertBuilder = feed.addEntityBuilder().setId(alert.get("id").toString() + ":" + startStr + ":" + endStr).getAlertBuilder();

                GtfsRealtime.TimeRange.Builder timeRange = alertBuilder.addActivePeriodBuilder();

                timeRange.setStart(startEpoch);
                timeRange.setEnd(endEpoch);
                alertBuilder.addActivePeriod(timeRange);

                // Set other properties for the alertBuilder (informed entities, cause, effect, etc.)
                for (Map.Entry<String, Object> lineEntry : lines.entrySet()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> line = (Map<String, Object>) lineEntry.getValue();

                    for (JsonNode impactedObject : (ArrayNode) line.get("impactedObjects")) {
                        boolean found = false;
                        for (int i = 0; i < ((ArrayNode) impactedObject.get("disruptionIds")).size(); i++) {
                            if (alert.get("id").equals(((ArrayNode) impactedObject.get("disruptionIds")).get(i).asText())) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            continue;
                        }

                        GtfsRealtime.EntitySelector.Builder entitySelector = alertBuilder.addInformedEntityBuilder();

                        String[] IdParts = impactedObject.get("id").asText().split(":");
                        String Id = String.join(":", Arrays.copyOfRange(IdParts, 1, IdParts.length)).replace("\"", "");

                        switch (impactedObject.get("type").asText()) {
                            case "line":
                                entitySelector.setRouteId(Id);
                                break;
                            case "stop_point":
                                entitySelector.setStopId(Id);
                                break;
                            case "stop_area":
                                entitySelector.setStopId(Id);
                                break;
                            default:
                                alertBuilder.removeInformedEntity(alertBuilder.getInformedEntityCount() - 1);
                                break;
                        }
                    }
                }

                // Set cause
                switch (alert.get("cause").toString()) {
                    case "TRAVAUX":
                        alertBuilder.setCause(GtfsRealtime.Alert.Cause.CONSTRUCTION);
                        break;
                    case "PERTURBATION":
                        alertBuilder.setCause(GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM);
                        break;
                    default:
                        alertBuilder.setCause(GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE);
                        break;
                }

                // Set effect
                switch (alert.get("severity").toString()) {
                    case "BLOQUANTE":
                        alertBuilder.setEffect(GtfsRealtime.Alert.Effect.NO_SERVICE);
                        break;
                    case "PERTURBEE":
                        alertBuilder.setEffect(GtfsRealtime.Alert.Effect.REDUCED_SERVICE);
                        break;
                    default:
                        alertBuilder.setEffect(GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT);
                        break;
                }

                // Set header text
                if (alert.get("title") != null) {
                    alertBuilder.setHeaderText(GtfsRealtime.TranslatedString.newBuilder().addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder().setText(alert.get("title").toString())));
                }

                // Set description text
                alertBuilder.setDescriptionText(GtfsRealtime.TranslatedString.newBuilder().addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder().setText(alert.get("message").toString())));

                // Set severity level
                switch (alert.get("severity").toString()) {
                    case "BLOQUANTE":
                        alertBuilder.setSeverityLevel(GtfsRealtime.Alert.SeverityLevel.SEVERE);
                        break;
                    case "PERTURBEE":
                        alertBuilder.setSeverityLevel(GtfsRealtime.Alert.SeverityLevel.WARNING);
                        break;
                    default:
                        alertBuilder.setSeverityLevel(GtfsRealtime.Alert.SeverityLevel.UNKNOWN_SEVERITY);
                        break;
                }
            }
        }

        // Build the feed message
        feed.setHeader(GtfsRealtime.FeedHeader.newBuilder().setGtfsRealtimeVersion("2.0").setIncrementality(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET).setTimestamp(System.currentTimeMillis()));
        try (FileOutputStream output = new FileOutputStream("gtfs-rt-alerts-idfm.pb")) {
            feed.build().writeTo(output);
        }

        System.out.println("Alerts generated successfully!");
    }

    private long convertToEpoch(String dateTimeStr) {
        // Update the formatter to match the date string format
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
        java.time.LocalDateTime dateTime = java.time.LocalDateTime.parse(dateTimeStr, formatter);
        java.time.ZoneId zoneId = java.time.ZoneId.of("Europe/Paris");
        return dateTime.atZone(zoneId).toEpochSecond();
    }

    public Map<String, Object> parseDisruptions(JsonNode disruptions) {
        Map<String, Object> alertDict = new HashMap<>();
    
        for (JsonNode disruption : disruptions) {
            String id = disruption.has("id") ? disruption.get("id").asText() : null;
            ArrayNode applicationPeriods = disruption.has("applicationPeriods") ? (ArrayNode) disruption.get("applicationPeriods") : null;
            String lastUpdate = disruption.has("lastUpdate") ? disruption.get("lastUpdate").asText() : null;
            String cause = disruption.has("cause") ? disruption.get("cause").asText() : null;
            String severity = disruption.has("severity") ? disruption.get("severity").asText() : null;
            ArrayNode tags = disruption.has("tags") ? (ArrayNode) disruption.get("tags") : null;
            String title = disruption.has("title") ? disruption.get("title").asText() : null;
            String message = disruption.has("message") ? disruption.get("message").asText() : null;
    
            // Skip disruptions without mandatory fields
            if (id == null || applicationPeriods == null) {
                System.err.println("Skipping disruption due to missing mandatory fields: id or applicationPeriods");
                continue;
            }
    
            Map<String, Object> alert = new HashMap<>();
            alert.put("id", id);
            alert.put("applicationPeriods", applicationPeriods);
            alert.put("lastUpdate", lastUpdate);
            alert.put("cause", cause);
            alert.put("severity", severity);
            alert.put("tags", tags);
            alert.put("title", title);
            alert.put("message", message);
    
            alertDict.put(id, alert);
        }
    
        return alertDict;
    }

    public Map<String, Object> parseLines(JsonNode lines) {
        Map<String, Object> linesDict = new HashMap<>();

        for (JsonNode line : lines) {
            String id = line.get("id").asText();
            String name = line.get("name").asText();
            String shortName = line.get("shortName").asText();
            String mode = line.get("mode").asText();
            String networkId = line.get("networkId").asText();
            ArrayNode impactedObjects = (ArrayNode) line.get("impactedObjects");

            Map<String, Object> lineDict = new HashMap<>();
            lineDict.put("id", id);
            lineDict.put("name", name);
            lineDict.put("shortName", shortName);
            lineDict.put("mode", mode);
            lineDict.put("networkId", networkId);
            lineDict.put("impactedObjects", impactedObjects);

            linesDict.put(id, lineDict);
        }

        return linesDict;
    }
}