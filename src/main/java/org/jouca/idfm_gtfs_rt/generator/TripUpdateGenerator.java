package org.jouca.idfm_gtfs_rt.generator;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jouca.idfm_gtfs_rt.fetchers.SiriLiteFetcher;
import org.jouca.idfm_gtfs_rt.finders.TripFinder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.transit.realtime.GtfsRealtime;

@Component
public class TripUpdateGenerator {
    private static final ZoneId ZONE_ID = ZoneId.of("Europe/Paris");
    private static final int PROGRESS_BAR_WIDTH = 40;

    @Value("${gtfsrt.debug.dump:false}")
    private boolean dumpDebugFiles;

    // Cache pour les temps parsés
    private final Map<String, Long> parsedTimeCache = new ConcurrentHashMap<>();
    private long currentEpochSecond = 0;

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

    public static Map<String, TripState> tripStates = new ConcurrentHashMap<>();
    // Optional reverse map to quickly know current trip for a vehicle (not strictly required but useful)
    public static Map<String, String> vehicleToTrip = new ConcurrentHashMap<>();

    private record IndexedEntity(int index, GtfsRealtime.FeedEntity entity) {}

    static class ProcessingContext {
        final ConcurrentMap<String, ConcurrentMap<Integer, RealtimeDirectionStats>> statsByRouteDirection = new ConcurrentHashMap<>();
    }

    static class RealtimeDirectionStats {
        final Set<String> tripIds = ConcurrentHashMap.newKeySet();
        volatile long maxStartTime = Long.MIN_VALUE;

        void addTrip(TripFinder.TripMeta meta) {
            if (meta == null) {
                return;
            }
            tripIds.add(meta.tripId);
            if (meta.firstTimeSecOfDay > maxStartTime) {
                maxStartTime = meta.firstTimeSecOfDay;
            }
        }
    }

    public void generateGTFSRT() throws Exception {
        // Initialiser le cache de temps au début de chaque génération
        parsedTimeCache.clear();
        currentEpochSecond = Instant.now().atZone(ZONE_ID).toEpochSecond();
        
        // Fetch SiriLite data
        JsonNode siriLiteData = SiriLiteFetcher.fetchSiriLiteData();

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

        ProcessingContext context = new ProcessingContext();

        // Parse SiriLite data and add it to the GTFS-RT feed
        processSiriLiteData(siriLiteData, feedMessage, context);

        // Append canceled trips based on theoretical schedule
        appendCanceledTrips(feedMessage, context);

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

    private void processSiriLiteData(JsonNode siriLiteData, GtfsRealtime.FeedMessage.Builder feedMessage, ProcessingContext context) {
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

        Map<String, JsonNode> entitiesTrips = dumpDebugFiles ? new ConcurrentHashMap<>() : null;

        int total = entities.size();
        renderProgressBar(0, total);

        ExecutorService executor = Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
        List<Future<IndexedEntity>> futures = new ArrayList<>(total);

        for (int idx = 0; idx < entities.size(); idx++) {
            final int index = idx;
            final JsonNode entity = entities.get(idx);
            futures.add(executor.submit((Callable<IndexedEntity>) () -> processEntity(entity, index, entitiesTrips, context)));
        }

        List<IndexedEntity> builtEntities = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            try {
                IndexedEntity result = futures.get(i).get();
                if (result != null && result.entity() != null) {
                    builtEntities.add(result);
                }
            } catch (Exception e) {
                System.err.println("Failed to process entity index " + i + ": " + e.getMessage());
            }
            renderProgressBar(i + 1, total);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

    builtEntities.stream()
        .sorted(Comparator.comparingInt(IndexedEntity::index))
        .forEach(indexed -> feedMessage.addEntity(indexed.entity()));

        // Clear tripStates where timestamp is older than 15 minutes
        long currentTime = Instant.now().atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();
        tripStates.entrySet().removeIf(entry -> currentTime - entry.getValue().lastUpdate > 15 * 60);
        
        // Clean vehicleToTrip entries referencing removed trip states
        vehicleToTrip.entrySet().removeIf(e -> !tripStates.containsKey(e.getValue()));

        System.out.println("Total trips in GTFS-RT feed: " + feedMessage.getEntityCount());

        // Export entitiesTrips to a JSON file
        if (dumpDebugFiles && entitiesTrips != null) {
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
    }

    void appendCanceledTrips(GtfsRealtime.FeedMessage.Builder feedMessage, ProcessingContext context) {
        if (context == null || context.statsByRouteDirection.isEmpty()) {
            return;
        }

        List<String> routeIds = new ArrayList<>(context.statsByRouteDirection.keySet());
        List<TripFinder.TripMeta> theoreticalTrips = TripFinder.getActiveTripsForRoutesToday(routeIds);
        if (theoreticalTrips == null || theoreticalTrips.isEmpty()) {
            return;
        }

        Map<String, Map<Integer, List<TripFinder.TripMeta>>> theoreticalByRouteDirection = theoreticalTrips.stream()
                .collect(Collectors.groupingBy(meta -> meta.routeId,
                        Collectors.groupingBy(meta -> meta.directionId)));

        Set<String> existingEntityIds = feedMessage.getEntityList().stream()
                .map(GtfsRealtime.FeedEntity::getId)
                .collect(Collectors.toCollection(java.util.HashSet::new));

        for (String routeId : routeIds) {
            Map<Integer, RealtimeDirectionStats> statsByDirection = context.statsByRouteDirection.get(routeId);
            if (statsByDirection == null || statsByDirection.isEmpty()) {
                continue;
            }

            Map<Integer, List<TripFinder.TripMeta>> theoreticalByDirection = theoreticalByRouteDirection.get(routeId);
            if (theoreticalByDirection == null || theoreticalByDirection.isEmpty()) {
                continue;
            }

            for (Map.Entry<Integer, RealtimeDirectionStats> entry : statsByDirection.entrySet()) {
                int directionId = entry.getKey();
                RealtimeDirectionStats stats = entry.getValue();
                if (stats == null || stats.tripIds.isEmpty()) {
                    continue;
                }

                List<TripFinder.TripMeta> candidates = theoreticalByDirection.get(directionId);
                if (candidates == null || candidates.isEmpty()) {
                    continue;
                }

                long cutoff = stats.maxStartTime;
                if (cutoff == Long.MIN_VALUE) {
                    continue;
                }

                candidates.stream()
                        .sorted(Comparator.comparingInt(meta -> meta.firstTimeSecOfDay))
                        .filter(meta -> meta.firstTimeSecOfDay <= cutoff)
                        .filter(meta -> !stats.tripIds.contains(meta.tripId))
                        .filter(meta -> existingEntityIds.add(meta.tripId))
                        .forEach(meta -> feedMessage.addEntity(buildCanceledEntity(meta)));
            }
        }
    }

    private GtfsRealtime.FeedEntity buildCanceledEntity(TripFinder.TripMeta meta) {
        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(meta.tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        tripUpdate.getTripBuilder()
                .setTripId(meta.tripId)
                .setRouteId(meta.routeId)
                .setDirectionId(meta.directionId)
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);

        return entityBuilder.build();
    }

    private IndexedEntity processEntity(JsonNode entity, int index, Map<String, JsonNode> entitiesTrips, ProcessingContext context) {
        String lineId = "IDFM:" + entity.get("LineRef").get("value").asText().split(":")[3];
        String vehicleId = entity.get("DatedVehicleJourneyRef").get("value").asText();

        // Determine direction from SIRI Lite data
        int direction = determineDirection(entity);
        Integer directionIdForMatching = (direction != -1) ? direction : null;

        String destinationIdCode = entity.get("DestinationRef").get("value").asText().split(":")[3];
        String destinationId = TripFinder.resolveStopId(destinationIdCode);

        // If not found, try to find it from the stop_extensions table
        if (destinationId == null) return null;

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
            } else if (call.has("AimedArrivalTime")) {
                isoTime = call.get("AimedArrivalTime").asText();
            } else if (call.has("AimedDepartureTime")) {
                isoTime = call.get("AimedDepartureTime").asText();
            }
            if (isoTime != null) {
                estimatedCallList.add(new org.jouca.idfm_gtfs_rt.records.EstimatedCall(stopId, isoTime));
            }
        }

        if (lineId == null || lineId.isEmpty() || estimatedCallList.isEmpty()) {
            return null;
        }

        // Use the new trip finder method
        boolean isArrivalTime = !estimatedCallList.isEmpty() && (estimatedCalls.get(0).has("ExpectedArrivalTime") || estimatedCalls.get(0).has("AimedArrivalTime"));

        // Get journey note
        String journeyNote = null;
        if (entity.has("JourneyNote") && entity.get("JourneyNote").size() > 0) {
            journeyNote = entity.get("JourneyNote").get(0).get("value").asText();
        }

        // Find the trip ID using the TripFinder utility
        final String tripId;
        String tmpTripId = null;
        try {
            tmpTripId = TripFinder.findTripIdFromEstimatedCalls(lineId, estimatedCallList, isArrivalTime, destinationId, journeyNote, directionIdForMatching);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        tripId = tmpTripId;

        if (tripId == null || tripId.isEmpty()) {
            return null; // Cannot proceed without stable trip id
        }

        TripFinder.TripMeta tripMeta = TripFinder.getTripMeta(tripId);
    if (context != null && tripMeta != null && tripMeta.routeId != null) {
            context.statsByRouteDirection
                    .computeIfAbsent(tripMeta.routeId, r -> new ConcurrentHashMap<>())
                    .computeIfAbsent(tripMeta.directionId, d -> new RealtimeDirectionStats())
                    .addTrip(tripMeta);
        }

        long now = Instant.now().atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();
        final String[] previousVehicleId = new String[1];
        TripState state = tripStates.compute(tripId, (id, existing) -> {
            if (existing == null) {
                return new TripState(tripId, vehicleId, now);
            }
            previousVehicleId[0] = existing.vehicleId;
            existing.lastUpdate = now;
            existing.vehicleId = vehicleId;
            return existing;
        });
        if (state == null) {
            state = new TripState(tripId, vehicleId, now);
            tripStates.put(tripId, state);
        }
        if (previousVehicleId[0] != null && !previousVehicleId[0].equals(vehicleId)) {
            vehicleToTrip.remove(previousVehicleId[0], tripId);
        }
        vehicleToTrip.put(vehicleId, tripId);

        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        int directionId = 0;
        if (tripMeta != null) {
            directionId = tripMeta.directionId;
        } else if (directionIdForMatching != null) {
            // Use the direction determined from SIRI Lite if available
            directionId = directionIdForMatching;
        } else {
            // Fallback: query from database
            String directionStr = TripFinder.getTripDirection(tripId);
            if (directionStr != null && !directionStr.isEmpty()) {
                try {
                    directionId = Integer.parseInt(directionStr);
                } catch (NumberFormatException ignored) {
                }
            }
        }

        String routeForDescriptor = tripMeta != null && tripMeta.routeId != null ? tripMeta.routeId : lineId;
        tripUpdate.getTripBuilder()
                .setRouteId(routeForDescriptor)
                .setDirectionId(directionId)
                .setTripId(tripId);
        // Always set latest vehicle id observed
        tripUpdate.getVehicleBuilder().setId(state.vehicleId);

        List<String> stopTimeUpdates = new ArrayList<>();

        // Check if all estimated calls are cancelled (either by DepartureStatus or ArrivalStatus)
        boolean allCancelled = estimatedCalls.stream().allMatch(call ->
            (call.has("DepartureStatus") && call.get("DepartureStatus").asText().contains("CANCELLED")) ||
            (call.has("ArrivalStatus") && call.get("ArrivalStatus").asText().contains("CANCELLED"))
        );
        if (allCancelled) {
            tripUpdate.getTripBuilder().setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        } else {
            // Process only real-time estimated calls from SIRI Lite (no theoretical times added)
            for (JsonNode estimatedCall : estimatedCalls) {
                processEstimatedCall(estimatedCall, tripUpdate, tripId, stopTimeUpdates);
            }
        }

        // Add/Update the trip to entitiesTrips (store last processed realtime entity snapshot)
        if (entitiesTrips != null) {
            entitiesTrips.put(tripId, entity);
        }
        return new IndexedEntity(index, entityBuilder.build());
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

            // IDFM cases
            if (directionName.equals("A")) {
                return 0;
            } else if (directionName.equals("R")) {
                return 1;
            }

            if (directionName.equals("Aller") || directionName.equals("inbound")) {
                return 1;
            } else if (directionName.equals("Retour") || directionName.equals("outbound")) {
                return 0;
            }
        }

        return -1; // Invalid direction
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
        // Utiliser le cache pour éviter de parser plusieurs fois le même timestamp
        return parsedTimeCache.computeIfAbsent(timeStr, ts -> {
            java.time.Instant instant = java.time.Instant.parse(ts);
            return instant.atZone(ZONE_ID).toEpochSecond();
        });
    }

    private List<JsonNode> getSortedEstimatedCalls(JsonNode entity) {
        List<JsonNode> estimatedCalls = new ArrayList<>();
        entity.get("EstimatedCalls").get("EstimatedCall").forEach(estimatedCalls::add);

        return estimatedCalls.stream()
                .sorted(Comparator.comparingLong(call -> {
                    String callTime = call.has("ExpectedArrivalTime") ? call.get("ExpectedArrivalTime").asText()
                            : call.has("ExpectedDepartureTime") ? call.get("ExpectedDepartureTime").asText() : 
                            call.has("AimedArrivalTime") ? call.get("AimedArrivalTime").asText() :
                            call.has("AimedDepartureTime") ? call.get("AimedDepartureTime").asText() : null;
                    return callTime != null ? Instant.parse(callTime).atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond() : Long.MAX_VALUE;
                }))
                .collect(Collectors.toList());
    }

    private void processEstimatedCall(JsonNode estimatedCall, GtfsRealtime.TripUpdate.Builder tripUpdate, String tripId, List<String> stopTimeUpdates) {
        // Check if times are after or equal to the current time (utiliser le cache)
        if (estimatedCall.has("ExpectedArrivalTime")) {
            long arrivalTime = parseTime(estimatedCall.get("ExpectedArrivalTime").asText());
            if (arrivalTime < currentEpochSecond) return;
        } else if (estimatedCall.has("AimedArrivalTime")) {
            long arrivalTime = parseTime(estimatedCall.get("AimedArrivalTime").asText());
            if (arrivalTime < currentEpochSecond) return;
        }

        if (estimatedCall.has("ExpectedDepartureTime")) {
            long departureTime = parseTime(estimatedCall.get("ExpectedDepartureTime").asText());
            if (departureTime < currentEpochSecond) return;
        } else if (estimatedCall.has("AimedDepartureTime")) {
            long departureTime = parseTime(estimatedCall.get("AimedDepartureTime").asText());
            if (departureTime < currentEpochSecond) return;
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
        } else if (estimatedCall.has("AimedArrivalTime")) {
            long arrivalTime = parseTime(estimatedCall.get("AimedArrivalTime").asText());
            stopTimeUpdate.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrivalTime).build());
        }

        if (estimatedCall.has("ExpectedDepartureTime")) {
            long departureTime = parseTime(estimatedCall.get("ExpectedDepartureTime").asText());
            stopTimeUpdate.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
        } else if (estimatedCall.has("AimedDepartureTime")) {
            long departureTime = parseTime(estimatedCall.get("AimedDepartureTime").asText());
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
    
        // Note: Le tri sera fait une seule fois à la fin du traitement de toutes les EstimatedCalls
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
}