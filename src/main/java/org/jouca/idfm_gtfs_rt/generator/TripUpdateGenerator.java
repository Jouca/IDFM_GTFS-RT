package org.jouca.idfm_gtfs_rt.generator;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

import org.jouca.idfm_gtfs_rt.fetchers.SiriLiteFetcher;
import org.jouca.idfm_gtfs_rt.finders.TripFinder;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.transit.realtime.GtfsRealtime;

@Component
public class TripUpdateGenerator {
    private static final ZoneId ZONE_ID = ZoneId.of("Europe/Paris");
    private static final int PROGRESS_BAR_WIDTH = 40;

    // Configurable window (in minutes) around now in which we add theoretical trips as canceled
    @Value("${gtfsrt.cancellation.window.minutes:120}")
    private int cancellationWindowMinutes;

    // Trip state keyed by stable theoretical tripId to handle changing vehicle identifiers in SIRI Lite
    public static class TripState {
        String tripId;
        String vehicleId; // last observed realtime vehicle identifier
        long lastUpdate;   // epoch seconds of last update

        TripState(String tripId, String vehicleId, long lastUpdate) {
            this.tripId = tripId;
            this.vehicleId = vehicleId;
            this.lastUpdate = lastUpdate;
        }
    }

    public static Map<String, TripState> tripStates = new HashMap<>();
    // Optional reverse map to quickly know current trip for a vehicle (not strictly required but useful)
    public static Map<String, String> vehicleToTrip = new HashMap<>();

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

    HashMap<String, JsonNode> entitiesTrips = new HashMap<>();
    
        int count = 0;
        int total = entities.size();
        renderProgressBar(0, total);
        for (JsonNode entity : entities) {
            processEntity(entity, feedMessage, 0, entitiesTrips);
            count++;
            //System.out.println(count + " / " + total + " entities processed.");
            renderProgressBar(count, total);
        }

        // Clear tripStates where timestamp is older than 15 minutes
        long currentTime = Instant.now().atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();
        tripStates.entrySet().removeIf(entry -> currentTime - entry.getValue().lastUpdate > 15 * 60);
        
        // Clean vehicleToTrip entries referencing removed trip states
        vehicleToTrip.entrySet().removeIf(e -> !tripStates.containsKey(e.getValue()));

        // After processing realtime, add canceled TripUpdates for theoretical trips not selected,
        // BUT only if at least one later realtime trip exists on the same route/direction
        try {
            // Determine all routeIds present in this SIRI batch to limit scope
            List<String> routeIdsInBatch = entities.stream().map(e -> "IDFM:" + e.get("LineRef").get("value").asText().split(":")[3])
                    .distinct().collect(Collectors.toList());

            List<TripFinder.TripMeta> todaysTrips = TripFinder.getActiveTripsForRoutesToday(routeIdsInBatch);

            // Build a set of tripIds already present in the feed
            java.util.Set<String> presentInFeed = new java.util.HashSet<>();
            // And compute the latest realtime firstTime per (routeId, directionId)
            java.util.Map<String, Integer> latestRealtimeFirstByRouteDir = new java.util.HashMap<>();
            long serviceDayEpoch = Instant.now().atZone(ZONE_ID).toLocalDate().atStartOfDay(ZONE_ID).toEpochSecond();
            for (int i = 0; i < feedMessage.getEntityCount(); i++) {
                GtfsRealtime.FeedEntity ent = feedMessage.getEntity(i);
                presentInFeed.add(ent.getId());
                if (ent.hasTripUpdate() && ent.getTripUpdate().hasTrip() && ent.getTripUpdate().getTrip().hasTripId()) {
                    String tId = ent.getTripUpdate().getTrip().getTripId();
                    TripFinder.TripMeta meta = TripFinder.getTripMeta(tId);
                    if (meta != null) {
                        String key = meta.routeId + "|" + meta.directionId;
                        latestRealtimeFirstByRouteDir.merge(key, meta.firstTimeSecOfDay, Integer::max);
                    }
                }
            }

            // Time window filter: only add cancellations for trips within +/- 2 hours of now
            long now = Instant.now().atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();
            long minTime = now - cancellationWindowMinutes * 60L;
            long maxTime = now + cancellationWindowMinutes * 60L;

            int addedCanceled = 0;
            for (TripFinder.TripMeta meta : todaysTrips) {
                if (presentInFeed.contains(meta.tripId)) continue; // already represented by realtime

                long firstTimeEpoch = serviceDayEpoch + meta.firstTimeSecOfDay;
                if (firstTimeEpoch < minTime || firstTimeEpoch > maxTime) continue; // outside window

                // Require existence of a later realtime trip for same (route, direction)
                String key = meta.routeId + "|" + meta.directionId;
                Integer latestRT = latestRealtimeFirstByRouteDir.get(key);
                if (latestRT == null) continue; // no realtime on this group; don't cancel
                if (latestRT <= meta.firstTimeSecOfDay) continue; // no later realtime; don't cancel

                // Append a canceled entity for this theoretical trip
                GtfsRealtime.FeedEntity.Builder canceledEntity = feedMessage.addEntityBuilder();
                canceledEntity.setId(meta.tripId);
                GtfsRealtime.TripUpdate.Builder tu = canceledEntity.getTripUpdateBuilder();
                tu.getTripBuilder()
                    .setTripId(meta.tripId)
                    .setRouteId(meta.routeId)
                    .setDirectionId(meta.directionId)
                    .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
                addedCanceled++;
            }
            if (addedCanceled > 0) {
                System.out.println("Added " + addedCanceled + " canceled theoretical trips not present in realtime.");
            }
        } catch (Exception ex) {
            System.err.println("Error while appending canceled trips: " + ex.getMessage());
        }

        System.out.println("Total trips in GTFS-RT feed: " + feedMessage.getEntityCount());

        // Export entitiesTrips to a JSON file
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
            com.fasterxml.jackson.databind.node.ObjectNode entitiesTripsJson = nodeFactory.objectNode();
            for (String tripId : entitiesTrips.keySet()) {
                entitiesTripsJson.set(tripId, entitiesTrips.get(tripId));
            }
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new java.io.File("entities_trips.json"), entitiesTripsJson);
            System.out.println("Entities trips written to entities_trips.json");
        } catch (Exception e) {
            System.err.println("Error writing entities trips to JSON: " + e.getMessage());
        }
    }

    private void processEntity(JsonNode entity, GtfsRealtime.FeedMessage.Builder feedMessage, int index, HashMap<String, JsonNode> entitiesTrips) {
        String lineId = "IDFM:" + entity.get("LineRef").get("value").asText().split(":")[3];
    String vehicleId = entity.get("DatedVehicleJourneyRef").get("value").asText();

        String destinationIdCode = entity.get("DestinationRef").get("value").asText().split(":")[3];
        String destinationId = TripFinder.resolveStopId(destinationIdCode);

        // If not found, try to find it from the stop_extensions table
        if (destinationId == null) return;

        //int direction = determineDirection(entity);
        //if (direction == -1) return;

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

        if (lineId == null || lineId.isEmpty() || estimatedCallList.isEmpty()) {
            return;
        }

        // Use the new trip finder method
        boolean isArrivalTime = !estimatedCallList.isEmpty() && estimatedCalls.get(0).has("ExpectedArrivalTime");

        // Get journey note
        String journeyNote = null;
        if (entity.has("JourneyNote") && entity.get("JourneyNote").size() > 0) {
            journeyNote = entity.get("JourneyNote").get(0).get("value").asText();
        }

        // Find the trip ID using the TripFinder utility
        final String tripId;
        String tmpTripId = null;
        try {
            tmpTripId = TripFinder.findTripIdFromEstimatedCalls(lineId, estimatedCallList, isArrivalTime, destinationId, journeyNote);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        tripId = tmpTripId;

        if (tripId == null || tripId.isEmpty()) {
            return; // Cannot proceed without stable trip id
        }

        long now = Instant.now().atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();
        TripState state = tripStates.get(tripId);
        if (state == null) {
            state = new TripState(tripId, vehicleId, now);
            tripStates.put(tripId, state);
            vehicleToTrip.put(vehicleId, tripId);
        } else {
            // Update state timestamp and vehicle id if changed
            state.lastUpdate = now;
            if (!state.vehicleId.equals(vehicleId)) {
                // Remove old vehicle mapping if still pointing to this trip
                vehicleToTrip.entrySet().removeIf(e -> e.getValue().equals(tripId));
                state.vehicleId = vehicleId;
                vehicleToTrip.put(vehicleId, tripId);
            }
        }

        // Either create or update feed entity keyed by tripId (stable, independent of changing vehicle IDs)
        GtfsRealtime.FeedEntity.Builder entityBuilder = null;
        int existingIndex = -1;
        for (int i = 0; i < feedMessage.getEntityCount(); i++) {
            if (feedMessage.getEntity(i).getId().equals(tripId)) {
                existingIndex = i;
                entityBuilder = feedMessage.getEntity(i).toBuilder();
                break;
            }
        }
        if (entityBuilder == null) {
            entityBuilder = feedMessage.addEntityBuilder();
            entityBuilder.setId(tripId);
        } else {
            // Clear existing stop time updates to rebuild them fresh
            if (entityBuilder.hasTripUpdate()) {
                entityBuilder.getTripUpdateBuilder().clearStopTimeUpdate();
            }
        }

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        tripUpdate.getTripBuilder()
                .setRouteId(lineId)
                .setDirectionId(Integer.parseInt(TripFinder.getTripDirection(tripId)))
                .setTripId(tripId);
        // Always set latest vehicle id observed
        tripUpdate.getVehicleBuilder().setId(state.vehicleId);

        List<String> stopTimeUpdates = new ArrayList<>();

        // Check if all estimated calls are cancelled
        // Check if all estimated calls are cancelled (either by DepartureStatus or ArrivalStatus)
        boolean allCancelled = estimatedCalls.stream().allMatch(call ->
            (call.has("DepartureStatus") && call.get("DepartureStatus").asText().contains("CANCELLED")) ||
            (call.has("ArrivalStatus") && call.get("ArrivalStatus").asText().contains("CANCELLED"))
        );
        if (allCancelled) {
            tripUpdate.getTripBuilder().setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        } else {
            // Ensure we are not carrying over a previous CANCELED relationship if situation recovered
            if (tripUpdate.getTripBuilder().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
                // No direct clear method may exist; recreate trip descriptor fields except schedule relationship
                // (If clearScheduleRelationship exists, we could call it. Keep safe by re-setting fields below.)
            }
            for (JsonNode estimatedCall : estimatedCalls) {
                processEstimatedCall(estimatedCall, tripUpdate, tripId, stopTimeUpdates);
                verifyStopUpdatesIntegrity(tripUpdate);
            }
        }

        // Fill all others stop_time_updates from the trip (count also the realtime delay during the trip)
        List<String> stopTimes = TripFinder.getAllStopTimesFromTrip(tripId);

        // Map : stop_sequence -> offset (carry-forward du dernier retard/avance réel)
        java.util.Map<Integer, Long> offsets = computeCarryForwardOffsets(estimatedCalls, stopTimes, tripId);

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
            Long offset = offsets.get(stopSeq);

            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = tripUpdate.addStopTimeUpdateBuilder();
            stopTimeUpdate.setStopSequence(stopSeq);
            stopTimeUpdate.setStopId(stopIdCollected);

            // Only apply offset if it was calculated for this stop
            if (arrivalTimeCollected != null) {
                long arrivalTime = serviceDayEpoch + (Long.parseLong(arrivalTimeCollected) % 86400);
                if (offset != null && offset != 0L) {
                    arrivalTime += offset;
                }
                stopTimeUpdate.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrivalTime).build());
            }
            if (departureTimeCollected != null) {
                long departureTime = serviceDayEpoch + (Long.parseLong(departureTimeCollected) % 86400);
                if (offset != null && offset != 0L) {
                    departureTime += offset;
                }
                stopTimeUpdate.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
            }
        }

        // 3. Vérifier et corriger l'ordre des horaires pour éviter les NEGATIVE_HOP_TIME/DWELL_TIME
        //    et garantir que les heures synthétiques respectent (horaire théorique + offset appliqué)
        //    afin d'éviter des plateaux.
        //    On construit donc un "plancher" (baseline) par stop_sequence.
        java.util.Map<Integer, Long> baselineArrival = new java.util.HashMap<>();
        java.util.Map<Integer, Long> baselineDeparture = new java.util.HashMap<>();
        long serviceDayEpochForBaseline = Instant.now().atZone(ZONE_ID)
                .toLocalDateTime()
                .toLocalDate()
                .atStartOfDay(ZONE_ID)
                .toEpochSecond();
        for (String st : stopTimes) {
            String[] parts = st.split(",");
            int seq = Integer.parseInt(parts[3]);
            Long off = offsets.get(seq);
            if (off == null) off = 0L;
            if (parts[1] != null && !parts[1].isEmpty()) {
                long baseArr = serviceDayEpochForBaseline + (Long.parseLong(parts[1]) % 86400) + off;
                baselineArrival.put(seq, baseArr);
            }
            if (parts[2] != null && !parts[2].isEmpty()) {
                long baseDep = serviceDayEpochForBaseline + (Long.parseLong(parts[2]) % 86400) + off;
                baselineDeparture.put(seq, baseDep);
            }
        }
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> sortedStopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream()
                .sorted(Comparator.comparingInt(GtfsRealtime.TripUpdate.StopTimeUpdate::getStopSequence))
                .collect(Collectors.toList());

        long lastTime = 0;
        final int MIN_INCREMENT_SECONDS = 1; // progression minimale pour éviter plusieurs arrêts avec la même heure
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> correctedUpdates = new ArrayList<>();
        for (GtfsRealtime.TripUpdate.StopTimeUpdate update : sortedStopTimeUpdates) {
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = update.toBuilder();
            if (update.hasArrival()) {
                long arr = update.getArrival().getTime();
                // imposer plancher = baseline arrival si dispo
                Long base = baselineArrival.get(update.getStopSequence());
                if (base != null && arr < base) {
                    arr = base;
                }
                if (arr <= lastTime) {
                    arr = lastTime + MIN_INCREMENT_SECONDS;
                    builder.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arr).build());
                }
                lastTime = arr;
            }
            if (update.hasDeparture()) {
                long dep = update.getDeparture().getTime();
                // imposer plancher = baseline departure si dispo
                Long base = baselineDeparture.get(update.getStopSequence());
                if (base != null && dep < base) {
                    dep = base;
                }
                if (dep <= lastTime) {
                    dep = lastTime + MIN_INCREMENT_SECONDS;
                    builder.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(dep).build());
                }
                lastTime = dep;
            }

            // Check if it's a skipped stop
            if (update.getScheduleRelationship() == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED) {
                // If the stop is skipped, we should not set arrival or departure times
                builder.clearArrival();
                builder.clearDeparture();
            }
            
            correctedUpdates.add(builder.build());
        }

        // Clear and rebuild the StopTimeUpdate list in the correct order
        tripUpdate.clearStopTimeUpdate();
        correctedUpdates.forEach(tripUpdate::addStopTimeUpdate);

        // Add/Update the trip to entitiesTrips (store last processed realtime entity snapshot)
        entitiesTrips.put(tripId, entity);

        // If we updated an existing entity, replace it in feedMessage
        if (existingIndex >= 0) {
            feedMessage.setEntity(existingIndex, entityBuilder.build());
        }
    }

    boolean checkStopIntegrity(JsonNode entity) {
        // Check if the stop is a integer
        if (entity.has("StopPointRef") && entity.get("StopPointRef").has("value")) {
            String stopPointRef = entity.get("StopPointRef").get("value").asText().split(":")[3];
            return stopPointRef.matches("\\d+"); // Check if the stop ID is an integer
        }

        return false; // Default to false if the check fails
    }

    // Removed unused determineDirection method; direction retrieved from TripFinder.getTripDirection(tripId)

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

    private void processEstimatedCall(JsonNode estimatedCall, GtfsRealtime.TripUpdate.Builder tripUpdate, String tripId, List<String> stopTimeUpdates) {
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

        // Check if skipped
        if (estimatedCall.has("DepartureStatus") && estimatedCall.get("DepartureStatus").asText().contains("CANCELLED")) {
            stopTimeUpdate.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);

            // Clear the arrival time if the departure is cancelled
            if (stopTimeUpdate.hasArrival()) {
                stopTimeUpdate.clearArrival();
            }
            if (stopTimeUpdate.hasDeparture()) {
                stopTimeUpdate.clearDeparture();
            }
        } else if (estimatedCall.has("ArrivalStatus") && estimatedCall.get("ArrivalStatus").asText().contains("CANCELLED")) {
            stopTimeUpdate.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);

            // Clear the departure time if the arrival is cancelled
            if (stopTimeUpdate.hasDeparture()) {
                stopTimeUpdate.clearDeparture();
            }
            if (stopTimeUpdate.hasArrival()) {
                stopTimeUpdate.clearArrival();
            }
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

    private void renderProgressBar(int current, int total) {
        if (total <= 0) {
            return;
        }

        int safeCurrent = Math.min(Math.max(current, 0), total);
        double progress = (double) safeCurrent / total;
        int filledLength = (int) Math.round(progress * PROGRESS_BAR_WIDTH);
        if (filledLength > PROGRESS_BAR_WIDTH) {
            filledLength = PROGRESS_BAR_WIDTH;
        }

        String filled = "=".repeat(filledLength);
        String empty = " ".repeat(PROGRESS_BAR_WIDTH - filledLength);
        int percentage = (int) Math.round(progress * 100);

        System.out.printf("\rProcessing entities: [%s%s] %3d%% (%d/%d)", filled, empty, percentage, safeCurrent, total);
        System.out.flush();

        if (safeCurrent >= total) {
            System.out.println();
        }
    }

    private void writeFeedToFile(GtfsRealtime.FeedMessage.Builder feedMessage, String filePath) {
        try (java.io.FileOutputStream outputStream = new java.io.FileOutputStream(filePath)) {
            feedMessage.build().writeTo(outputStream);
            System.out.println("GTFS-RT feed written to " + filePath);
        } catch (java.io.IOException e) {
            System.err.println("Error writing GTFS-RT feed: " + e.getMessage());
        }
    }

    /**
     * Logique demandée : pour chaque arrêt sans temps réel, on applique le dernier offset connu.
     * (scheduled time + dernier retard/avance observé). Pas d'interpolation progressive.
     */
    private java.util.Map<Integer, Long> computeCarryForwardOffsets(List<JsonNode> estimatedCalls, List<String> scheduledStopTimes, String tripId) {
        java.util.Map<Integer, Long> offsets = new java.util.HashMap<>();

        long currentOffset = 0L; // défaut si aucun temps réel encore

        // Pré-indexation des stopTimes par (stop_id, seq) pour éviter recalculs
        java.util.Map<String, String> stopIdBySeq = new java.util.HashMap<>();
        for (String st : scheduledStopTimes) {
            String[] parts = st.split(",");
            stopIdBySeq.put(parts[3], parts[0]);
        }

        // Récupère les sequences triées pour itération finale
        java.util.List<Integer> allSequences = scheduledStopTimes.stream()
                .map(s -> Integer.parseInt(s.split(",")[3]))
                .sorted()
                .collect(java.util.stream.Collectors.toList());

        // Map sequence -> offset réel calculé (points d'ancrage)
        java.util.Map<Integer, Long> anchorOffsets = new java.util.HashMap<>();

        for (JsonNode ec : estimatedCalls) {
            boolean cancelled = (ec.has("DepartureStatus") && ec.get("DepartureStatus").asText().contains("CANCELLED")) ||
                                (ec.has("ArrivalStatus") && ec.get("ArrivalStatus").asText().contains("CANCELLED"));
            if (cancelled) continue;

            String stopId = TripFinder.resolveStopId(ec.get("StopPointRef").get("value").asText().split(":")[3]);
            if (stopId == null) continue;
            String seqStr = TripFinder.findStopSequence(tripId, stopId, new java.util.ArrayList<>());
            if (seqStr == null) continue;
            int seq = Integer.parseInt(seqStr);

            Long scheduled = null; Long realtime = null;
            if (ec.has("ExpectedArrivalTime")) {
                scheduled = TripFinder.getScheduledArrivalTime(scheduledStopTimes, stopId, seqStr, ZONE_ID);
                realtime = parseTime(ec.get("ExpectedArrivalTime").asText());
            } else if (ec.has("ExpectedDepartureTime")) {
                scheduled = TripFinder.getScheduledDepartureTime(scheduledStopTimes, stopId, seqStr, ZONE_ID);
                realtime = parseTime(ec.get("ExpectedDepartureTime").asText());
            }
            if (scheduled != null && realtime != null) {
                long offset = realtime - scheduled;
                anchorOffsets.put(seq, offset);
            }
        }

        // Itération dans l'ordre des séquences : on propage le dernier offset connu
        for (Integer seq : allSequences) {
            if (anchorOffsets.containsKey(seq)) {
                currentOffset = anchorOffsets.get(seq);
            }
            offsets.put(seq, currentOffset);
        }

        return offsets;
    }
}