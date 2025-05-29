package org.jouca.idfm_gtfs_rt.generator;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.jouca.idfm_gtfs_rt.fetchers.SiriLiteFetcher;
import org.jouca.idfm_gtfs_rt.finders.TripFinder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.transit.realtime.GtfsRealtime;

@Component
public class TripUpdateGenerator {
    private static final ZoneId ZONE_ID = ZoneId.of("Europe/Paris");

    public void generateGTFSRT() throws Exception {
        // Fetch SiriLite data
        JsonNode siriLiteData = SiriLiteFetcher.fetchSiriLiteData();

        // Save SiriLite data to a file
        saveSiriLiteDataToFile(siriLiteData, "sirilite_data.json");

        // Check if stop_extensions table exists in SQLite database
        if (!TripFinder.checkIfStopExtensionsTableExists()) {
            return;
        }

        // Create GTFS-RT feed
        GtfsRealtime.FeedMessage.Builder feedMessage = GtfsRealtime.FeedMessage.newBuilder();
        feedMessage.setHeader(GtfsRealtime.FeedHeader.newBuilder()
                .setGtfsRealtimeVersion("2.0")
                .setIncrementality(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET)
                .setTimestamp(System.currentTimeMillis() / 1000L));

        // Parse SiriLite data and add it to the GTFS-RT feed
        processSiriLiteData(siriLiteData, feedMessage);

        // Write the GTFS-RT feed to a file
        writeFeedToFile(feedMessage, "gtfs-rt-trips-idfm.pb");
    }

    private void saveSiriLiteDataToFile(JsonNode siriLiteData, String filePath) {
        try (java.io.FileOutputStream outputStream = new java.io.FileOutputStream(filePath)) {
            outputStream.write(siriLiteData.toString().getBytes());
            System.out.println("SiriLite data written to " + filePath);
        } catch (java.io.IOException e) {
            System.err.println("Error writing SiriLite data: " + e.getMessage());
        }
    }

    private void processSiriLiteData(JsonNode siriLiteData, GtfsRealtime.FeedMessage.Builder feedMessage) {
        List<JsonNode> entities = new ArrayList<>();
        siriLiteData.get("Siri").get("ServiceDelivery").get("EstimatedTimetableDelivery").get(0)
                .get("EstimatedJourneyVersionFrame").get(0).get("EstimatedVehicleJourney").forEach(entities::add);

        System.out.println(entities.size() + " entities found in SiriLite data.");
    
        // Sort entities by the earliest departure and arrival times
        entities.sort(Comparator.comparingLong(entity -> {
            JsonNode estimatedCalls = entity.get("EstimatedCalls").get("EstimatedCall");
            if (estimatedCalls != null && estimatedCalls.size() > 0) {
                JsonNode firstCall = estimatedCalls.get(0);
                String time = null;
                if (firstCall.has("ExpectedDepartureTime")) {
                    time = firstCall.get("ExpectedDepartureTime").asText();
                } else if (firstCall.has("ExpectedArrivalTime")) {
                    time = firstCall.get("ExpectedArrivalTime").asText();
                } else if (firstCall.has("AimedDepartureTime")) {
                    time = firstCall.get("AimedDepartureTime").asText();
                } else if (firstCall.has("AimedArrivalTime")) {
                    time = firstCall.get("AimedArrivalTime").asText();
                }
                if (time != null) {
                    return Instant.parse(time)
                        .atZone(ZONE_ID)
                        .toLocalDateTime()
                        .atZone(ZONE_ID)
                        .toEpochSecond();
                }
            }
            return Long.MAX_VALUE; // Default to max value if no time is available
        }));

        System.out.println("Processing " + entities.size() + " entities...");

        List<String> tripIds = new ArrayList<>();
    
        int count = 0;
        int total = entities.size();
        for (JsonNode entity : entities) {
            try {
                processEntity(entity, feedMessage, 0, tripIds);
                count++;
                //System.out.println(count + " / " + total + " entities processed.");
            } catch (Exception e) {
                // Log or handle the exception if needed
                System.err.println("Error processing entity: " + e.getMessage());
            }
        }
    }

    private void processEntity(JsonNode entity, GtfsRealtime.FeedMessage.Builder feedMessage, int index, List<String> tripIds) throws Exception {
        String lineId = "IDFM:" + entity.get("LineRef").get("value").asText().split(":")[3];
        String destinationIdCode = entity.get("DestinationRef").get("value").asText().split(":")[3];
        String destinationId = TripFinder.resolveStopId(destinationIdCode);

        // If not found, try to find it from the stop_extensions table
        if (destinationId == null) return;

        String vehicleId = entity.get("DatedVehicleJourneyRef").get("value").asText();

        int direction = determineDirection(entity);
        if (direction == -1) return;

        List<JsonNode> estimatedCalls = getSortedEstimatedCalls(entity);

        // Build EstimatedCall list for trip search
        List<org.jouca.idfm_gtfs_rt.records.EstimatedCall> estimatedCallList = new ArrayList<>();
        for (JsonNode call : estimatedCalls) {
            String stopCode = call.get("StopPointRef").get("value").asText().split(":")[3];
            String stopId = TripFinder.resolveStopId(stopCode);
            if (stopId == null) continue;

            String isoTime = null;
            if (call.has("ExpectedArrivalTime")) {
                isoTime = call.get("ExpectedArrivalTime").asText();
            } else if (call.has("ExpectedDepartureTime")) {
                isoTime = call.get("ExpectedDepartureTime").asText();
            }
            if (isoTime != null) {
                estimatedCallList.add(new org.jouca.idfm_gtfs_rt.records.EstimatedCall(stopId, isoTime));
            }
        }

        if (lineId == null || lineId.isEmpty() || estimatedCallList.isEmpty() || direction == -1) {
            return;
        }

        // Use the new trip finder method
        boolean isArrivalTime = !estimatedCallList.isEmpty() && estimatedCalls.get(0).has("ExpectedArrivalTime");
        String tripId = TripFinder.findTripIdFromEstimatedCalls(lineId, String.valueOf(direction), estimatedCallList, isArrivalTime, tripIds);

        //if (lineId.equals("IDFM:C01063")) System.out.println("Processing entity with lineId: " + lineId + ", direction: " + direction + ", estimatedCalls: " + estimatedCallList + ", tripId: " + tripId);

        if (tripId == null) {
            return; // Skip this entity if no trip ID is found
        }

        // Add trip ID to the list to avoid duplicates
        tripIds.add(tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = feedMessage.addEntityBuilder().setId(vehicleId).getTripUpdateBuilder();
        tripUpdate.getTripBuilder()
                .setRouteId(lineId)
                .setDirectionId(direction)
                .setTripId(tripId);
        tripUpdate.getVehicleBuilder().setId(vehicleId);

        List<String> stopTimeUpdates = new ArrayList<>();

        // Get status from the first estimated call
        JsonNode entityEstimatedCalls = estimatedCalls.get(index);
        String departureStatus = entityEstimatedCalls.has("DepartureStatus") ? entityEstimatedCalls.get("DepartureStatus").asText() : null;
        String arrivalStatus = entityEstimatedCalls.has("ArrivalStatus") ? entityEstimatedCalls.get("ArrivalStatus").asText() : null;

        // Handle cancellation
        if (departureStatus != null && departureStatus.contains("CANCELLED")) {
            tripUpdate.getTripBuilder().setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        } else if (arrivalStatus != null && arrivalStatus.contains("CANCELLED")) {
            tripUpdate.getTripBuilder().setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        } else {
            for (JsonNode estimatedCall : estimatedCalls) {
                processEstimatedCall(estimatedCall, tripUpdate, tripId, stopTimeUpdates);
                verifyStopUpdatesIntegrity(tripUpdate);
            }
        }

        // Fill all others stop_time_updates from the trip (count also the realtime delay during the trip)
        List<String> stopTimes = TripFinder.getAllStopTimesFromTrip(tripId);

        // Map : stop_sequence -> offset à appliquer (en secondes)
        java.util.Map<Integer, Long> offsets = new java.util.HashMap<>();
        long lastOffset = 0;
        int lastSequence = -1;

        // Construction de la map des offsets à partir des EstimatedCalls temps réel
        for (JsonNode estimatedCall : estimatedCalls) {
            String stopId = TripFinder.resolveStopId(estimatedCall.get("StopPointRef").get("value").asText().split(":")[3]);
            if (stopId == null) continue;
            String stopSequenceStr = TripFinder.findStopSequence(tripId, stopId, new ArrayList<>());
            if (stopSequenceStr == null) continue;
            int stopSequence = Integer.parseInt(stopSequenceStr);

            Long scheduledTime = null;
            Long realTime = null;
            if (estimatedCall.has("ExpectedArrivalTime")) {
                scheduledTime = TripFinder.getScheduledArrivalTime(stopTimes, stopId, stopSequenceStr, ZONE_ID);
                realTime = parseTime(estimatedCall.get("ExpectedArrivalTime").asText());
            } else if (estimatedCall.has("ExpectedDepartureTime")) {
                scheduledTime = TripFinder.getScheduledDepartureTime(stopTimes, stopId, stopSequenceStr, ZONE_ID);
                realTime = parseTime(estimatedCall.get("ExpectedDepartureTime").asText());
            }
            if (scheduledTime != null && realTime != null) {
                long offset = realTime - scheduledTime;
                // Appliquer l'offset à tous les arrêts jusqu'à ce point (y compris les précédents)
                for (int seq = lastSequence + 1; seq <= stopSequence; seq++) {
                    offsets.put(seq, offset);
                }
                lastOffset = offset;
                lastSequence = stopSequence;
            }
        }
        // Appliquer le dernier offset à tous les arrêts suivants
        int maxSeq = stopTimes.size();
        for (int seq = lastSequence + 1; seq <= maxSeq; seq++) {
            offsets.put(seq, lastOffset);
        }

        // 2. Générer les StopTimeUpdate en appliquant l'offset correct
        for (String stopTime : stopTimes) {
            ArrayList<String> stopTimeParts = new ArrayList<>(List.of(stopTime.split(",")));
            String stopIdCollected = stopTimeParts.get(0);
            String arrivalTimeCollected = stopTimeParts.get(1);
            String departureTimeCollected = stopTimeParts.get(2);
            String stopSequenceCollected = stopTimeParts.get(3);

            boolean alreadyExists = false;

            long serviceDayEpoch = Instant.now().atZone(ZONE_ID)
                    .toLocalDateTime()
                    .toLocalDate()
                    .atStartOfDay(ZONE_ID)
                    .toEpochSecond();

            for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : tripUpdate.getStopTimeUpdateList()) {
                if (stopTimeUpdate.getStopSequence() == Integer.parseInt(stopSequenceCollected)) {
                    alreadyExists = true;
                    break;
                }
            }

            if (alreadyExists) {
                continue;
            }

            int stopSeq = Integer.parseInt(stopSequenceCollected);
            long offset = offsets.getOrDefault(stopSeq, 0L);

            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = tripUpdate.addStopTimeUpdateBuilder();
            stopTimeUpdate.setStopSequence(stopSeq);
            stopTimeUpdate.setStopId(stopIdCollected);
            if (arrivalTimeCollected != null) {
                long arrivalTime = serviceDayEpoch + (Long.parseLong(arrivalTimeCollected) % 86400) + offset;
                stopTimeUpdate.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrivalTime).build());
            }
            if (departureTimeCollected != null) {
                long departureTime = serviceDayEpoch + (Long.parseLong(departureTimeCollected) % 86400) + offset;
                stopTimeUpdate.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
            }
        }

        // 3. Vérifier et corriger l'ordre des horaires pour éviter les NEGATIVE_HOP_TIME/DWELL_TIME
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> sortedStopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream()
                .sorted(Comparator.comparingInt(GtfsRealtime.TripUpdate.StopTimeUpdate::getStopSequence))
                .collect(Collectors.toList());

        long lastTime = 0;
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> correctedUpdates = new ArrayList<>();
        for (GtfsRealtime.TripUpdate.StopTimeUpdate update : sortedStopTimeUpdates) {
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = update.toBuilder();
            if (update.hasArrival()) {
                long arr = update.getArrival().getTime();
                if (arr < lastTime) {
                    arr = lastTime;
                    builder.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arr).build());
                }
                lastTime = arr;
            }
            if (update.hasDeparture()) {
                long dep = update.getDeparture().getTime();
                if (dep < lastTime) {
                    dep = lastTime;
                    builder.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(dep).build());
                }
                lastTime = dep;
            }
            correctedUpdates.add(builder.build());
        }

        // Clear and rebuild the StopTimeUpdate list in the correct order
        tripUpdate.clearStopTimeUpdate();
        correctedUpdates.forEach(tripUpdate::addStopTimeUpdate);
    }

    boolean checkStopIntegrity(JsonNode entity) {
        // Check if the stop is a integer
        if (entity.has("StopPointRef") && entity.get("StopPointRef").has("value")) {
            String stopPointRef = entity.get("StopPointRef").get("value").asText().split(":")[3];
            return stopPointRef.matches("\\d+"); // Check if the stop ID is an integer
        }

        return false; // Default to false if the check fails
    }

    private int determineDirection(JsonNode entity) {
        if (entity.has("DirectionRef") && entity.get("DirectionRef").has("value")) {
            String directionValue = entity.get("DirectionRef").get("value").asText();
            if (directionValue.contains(":")) {
                String[] directionParts = directionValue.split(":");
                if (directionParts.length > 3) {
                    String directionString = directionParts[3];
                    if ("A".equals(directionString)) {
                        return 1;
                    } else if ("R".equals(directionString)) {
                        return 0;
                    }
                }
            } else {
                if ("Aller".equals(directionValue) || "inbound".equals(directionValue) || "A".equals(directionValue)) {
                    return 1;
                } else if ("Retour".equals(directionValue) || "outbound".equals(directionValue) || "R".equals(directionValue)) {
                    return 0;
                }
            }
        } 
        
        if (entity.has("DirectionName") && entity.get("DirectionName").size() > 0) {
            String directionName = entity.get("DirectionName").get(0).get("value").asText();
            if (directionName.equals("Aller") || directionName.equals("A") || directionName.equals("inbound")) {
                return 1;
            } else if (directionName.equals("Retour") || directionName.equals("R") || directionName.equals("outbound")) {
                return 0;
            }
        }
        return -1; // Invalid direction
    }

    private long parseTime(String timeStr) {
        java.time.Instant instant = java.time.Instant.parse(timeStr);
        return instant.atZone(ZONE_ID).toEpochSecond();
    }

    private List<JsonNode> getSortedEstimatedCalls(JsonNode entity) {
        List<JsonNode> estimatedCalls = new ArrayList<>();
        entity.get("EstimatedCalls").get("EstimatedCall").forEach(estimatedCalls::add);

        return estimatedCalls.stream()
                .sorted(Comparator.comparingLong(call -> {
                    String callTime = call.has("ExpectedArrivalTime") ? call.get("ExpectedArrivalTime").asText()
                            : call.has("ExpectedDepartureTime") ? call.get("ExpectedDepartureTime").asText() : null;
                    return callTime != null ? Instant.parse(callTime).atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond() : Long.MAX_VALUE;
                }))
                .collect(Collectors.toList());
    }

    private void processEstimatedCall(JsonNode estimatedCall, GtfsRealtime.TripUpdate.Builder tripUpdate, String tripId, List<String> stopTimeUpdates) throws Exception {
        // Check if times are after or equal to the current time
        if (estimatedCall.has("ExpectedArrivalTime")) {
            long arrivalTime = parseTime(estimatedCall.get("ExpectedArrivalTime").asText());
            if (arrivalTime < Instant.now().atZone(ZONE_ID).toEpochSecond()) return;
        }
        if (estimatedCall.has("ExpectedDepartureTime")) {
            long departureTime = parseTime(estimatedCall.get("ExpectedDepartureTime").asText());
            if (departureTime < Instant.now().atZone(ZONE_ID).toEpochSecond()) return;
        }

        String stopId = TripFinder.resolveStopId(estimatedCall.get("StopPointRef").get("value").asText().split(":")[3]);
        if (stopId == null) return;

        String stopSequence = TripFinder.findStopSequence(tripId, stopId, stopTimeUpdates);
        if (stopSequence == null) return;

        stopTimeUpdates.add(stopSequence);

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = tripUpdate.addStopTimeUpdateBuilder();
        stopTimeUpdate.setStopSequence(Integer.parseInt(stopSequence));
        stopTimeUpdate.setStopId(stopId);

        if (estimatedCall.has("ExpectedArrivalTime")) {
            long arrivalTime = parseTime(estimatedCall.get("ExpectedArrivalTime").asText());
            stopTimeUpdate.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrivalTime).build());
        }

        if (estimatedCall.has("ExpectedDepartureTime")) {
            long departureTime = parseTime(estimatedCall.get("ExpectedDepartureTime").asText());
            stopTimeUpdate.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
        }
    
        // Reorder StopTimeUpdates by stop_sequence
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> sortedStopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream()
                .sorted(Comparator.comparingInt(GtfsRealtime.TripUpdate.StopTimeUpdate::getStopSequence))
                .collect(Collectors.toList());
    
        // Clear and rebuild the StopTimeUpdate list in the correct order
        tripUpdate.clearStopTimeUpdate();
        sortedStopTimeUpdates.forEach(tripUpdate::addStopTimeUpdate);
    }

    private void verifyStopUpdatesIntegrity(GtfsRealtime.TripUpdate.Builder tripUpdate) {
        // Function for checking if times on stop updates are chronologically correct. Remove any that are not.
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = tripUpdate.getStopTimeUpdateList();
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> validUpdates = new ArrayList<>();

        for (int i = 0; i < stopTimeUpdates.size(); i++) {
            GtfsRealtime.TripUpdate.StopTimeUpdate current = stopTimeUpdates.get(i);
            if (i < stopTimeUpdates.size() - 1) {
                GtfsRealtime.TripUpdate.StopTimeUpdate next = stopTimeUpdates.get(i + 1);

                if (current.hasArrival() && next.hasArrival() && current.getArrival().getTime() > next.getArrival().getTime()) {
                    continue; // Skip invalid update
                }

                if (current.hasDeparture() && next.hasDeparture() && current.getDeparture().getTime() > next.getDeparture().getTime()) {
                    continue; // Skip invalid update
                }

                if (current.hasArrival() && current.hasDeparture() &&
                    current.getArrival().getTime() > current.getDeparture().getTime()) {
                    continue; // Skip invalid update
                }
            }
            validUpdates.add(current);
        }

        // Clear and replace the StopTimeUpdate list with valid updates
        tripUpdate.clearStopTimeUpdate();
        validUpdates.forEach(tripUpdate::addStopTimeUpdate);
    }

    private void writeFeedToFile(GtfsRealtime.FeedMessage.Builder feedMessage, String filePath) {
        try (java.io.FileOutputStream outputStream = new java.io.FileOutputStream(filePath)) {
            feedMessage.build().writeTo(outputStream);
            System.out.println("GTFS-RT feed written to " + filePath);
        } catch (java.io.IOException e) {
            System.err.println("Error writing GTFS-RT feed: " + e.getMessage());
        }
    }
}