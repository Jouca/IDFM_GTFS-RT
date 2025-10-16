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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jouca.idfm_gtfs_rt.fetchers.SiriLiteFetcher;
import org.jouca.idfm_gtfs_rt.finders.TripFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.transit.realtime.GtfsRealtime;

/**
 * Generator for GTFS Realtime TripUpdate feeds from SIRI Lite data.
 * 
 * <p>This component is responsible for:
 * <ul>
 *   <li>Fetching real-time transit data from SIRI Lite API</li>
 *   <li>Matching real-time data with theoretical GTFS trips</li>
 *   <li>Generating GTFS-RT TripUpdate messages with stop time predictions</li>
 *   <li>Detecting and marking canceled trips</li>
 *   <li>Managing vehicle-to-trip associations across updates</li>
 * </ul>
 * 
 * <p>The generator uses parallel processing to handle large volumes of real-time data
 * efficiently and maintains state to track vehicle assignments across feed updates.
 * 
 * @author Jouca
 * @since 1.0
 * 
 * @see SiriLiteFetcher
 * @see TripFinder
 */
@Component
public class TripUpdateGenerator {
    private static final Logger logger = LoggerFactory.getLogger(TripUpdateGenerator.class);
    
    /** Time zone for Paris, used for all time conversions */
    private static final ZoneId ZONE_ID = ZoneId.of("Europe/Paris");
    
    /** Width of the progress bar displayed during processing */
    private static final int PROGRESS_BAR_WIDTH = 40;

    /** Flag to enable debug file output (configured via application properties) */
    @Value("${gtfsrt.debug.dump:false}")
    private boolean dumpDebugFiles;

    /** Cache for parsed ISO 8601 timestamps to avoid repeated parsing operations */
    private final Map<String, Long> parsedTimeCache = new ConcurrentHashMap<>();
    
    /** Current epoch second, initialized at the start of each feed generation */
    private long currentEpochSecond = 0;

    /**
     * Represents the current state of a trip in the real-time system.
     * 
     * <p>Trip state is keyed by stable theoretical trip ID to handle cases where
     * the vehicle identifier changes between updates (common in SIRI Lite data).
     * This allows the system to maintain continuity for the same trip even when
     * the vehicle assignment changes.
     */
    public static class TripState {
        /** GTFS trip ID (theoretical/scheduled trip identifier) */
        String tripId;
        
        /** Current real-time vehicle identifier from SIRI Lite */
        String vehicleId;
        
        /** Timestamp of last update (epoch seconds) */
        long lastUpdate;

        /**
         * Constructs a new TripState.
         * 
         * @param tripId the GTFS trip identifier
         * @param vehicleId the real-time vehicle identifier
         * @param lastUpdate the timestamp of this update (epoch seconds)
         */
        TripState(String tripId, String vehicleId, long lastUpdate) {
            this.tripId = tripId;
            this.vehicleId = vehicleId;
            this.lastUpdate = lastUpdate;
        }
    }

    /**
     * Global map of trip states indexed by trip ID.
     * Thread-safe for concurrent updates during parallel processing.
     */
    public static Map<String, TripState> tripStates = new ConcurrentHashMap<>();
    
    /**
     * Reverse mapping from vehicle ID to trip ID for quick lookups.
     * Useful for determining which trip a vehicle is currently serving.
     */
    public static Map<String, String> vehicleToTrip = new ConcurrentHashMap<>();

    /**
     * Internal record to maintain the original processing order of entities.
     * Used to preserve the sorted order after parallel processing.
     */
    private record IndexedEntity(int index, GtfsRealtime.FeedEntity entity) {}

    /**
     * Context object for collecting statistics during feed processing.
     * Used to identify which trips are present in real-time data for each route/direction
     * combination, enabling detection of canceled trips.
     */
    static class ProcessingContext {
        /** Statistics indexed by route ID and direction ID */
        final ConcurrentMap<String, ConcurrentMap<Integer, RealtimeDirectionStats>> statsByRouteDirection = new ConcurrentHashMap<>();
    }

    /**
     * Statistics for a specific route and direction in the real-time data.
     * Tracks which trips have been observed and the latest start time,
     * used to determine which theoretical trips should be marked as canceled.
     */
    static class RealtimeDirectionStats {
        /** Set of trip IDs observed in real-time data */
        final Set<String> tripIds = ConcurrentHashMap.newKeySet();
        
        /** Maximum start time observed for trips in this direction (seconds of day) */
        volatile long maxStartTime = Long.MIN_VALUE;

        /**
         * Adds a trip to the statistics.
         * 
         * @param meta the trip metadata containing trip ID and start time
         */
        /**
         * Adds a trip to the statistics.
         * 
         * @param meta the trip metadata containing trip ID and start time
         */
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

    /**
     * Main entry point for generating GTFS-RT feed.
     * 
     * <p>This method orchestrates the entire feed generation process:
     * <ol>
     *   <li>Fetches real-time SIRI Lite data from the transit agency API</li>
     *   <li>Validates that required database tables exist</li>
     *   <li>Creates a GTFS-RT FeedMessage with appropriate headers</li>
     *   <li>Processes all real-time vehicle journeys in parallel</li>
     *   <li>Identifies and marks canceled trips</li>
     *   <li>Writes the completed feed to a Protocol Buffer file</li>
     * </ol>
     * 
     * @throws Exception if data fetching, processing, or file I/O fails
     */
    public void generateGTFSRT() throws Exception {
        // Initialize the time cache at the start of each generation
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

    /**
     * Saves SIRI Lite data to a JSON file for debugging purposes.
     * 
     * @param siriLiteData the JSON node containing SIRI Lite data
     * @param filePath the output file path
     */
    private void saveSiriLiteDataToFile(JsonNode siriLiteData, String filePath) {
        try (java.io.FileOutputStream outputStream = new java.io.FileOutputStream(filePath)) {
            outputStream.write(siriLiteData.toString().getBytes());
            System.out.println("SiriLite data written to " + filePath);
        } catch (java.io.IOException e) {
            logger.error("Error writing SiriLite data: {}", e.getMessage(), e);
        }
    }

    /**
     * Processes SIRI Lite data and converts it to GTFS-RT TripUpdate entities.
     * 
     * <p>This method performs the following operations:
     * <ul>
     *   <li>Extracts EstimatedVehicleJourney entities from SIRI Lite data</li>
     *   <li>Sorts entities by earliest departure/arrival time</li>
     *   <li>Processes entities in parallel using a thread pool</li>
     *   <li>Maintains original order in the output feed</li>
     *   <li>Cleans up stale trip states (older than 15 minutes)</li>
     *   <li>Optionally exports debug information</li>
     * </ul>
     * 
     * @param siriLiteData the JSON data from SIRI Lite API
     * @param feedMessage the GTFS-RT feed message builder to populate
     * @param context processing context for tracking trip statistics
     */
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
        List<IndexedEntity> builtEntities = new ArrayList<>();
        try {
            List<Future<IndexedEntity>> futures = new ArrayList<>(total);

            for (int idx = 0; idx < entities.size(); idx++) {
                final int index = idx;
                final JsonNode entity = entities.get(idx);
                futures.add(executor.submit((Callable<IndexedEntity>) () -> processEntity(entity, index, entitiesTrips, context)));
            }

            builtEntities = collectFutureResults(futures, total);

            builtEntities.stream()
                .sorted(Comparator.comparingInt(IndexedEntity::index))
                .forEach(indexed -> feedMessage.addEntity(indexed.entity()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status
            logger.error("Thread interrupted during parallel processing of {} SIRI Lite entities: {}", total, e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.error("Execution error during parallel processing of {} SIRI Lite entities: {}", total, e.getMessage(), e);
        } finally {
            shutdownExecutor(executor);
        }

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
                logger.error("Error writing entities trips to JSON: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Appends canceled trip entities to the GTFS-RT feed.
     * 
     * <p>This method identifies theoretical trips that should be running based on the
     * schedule but are missing from the real-time data. For each route and direction
     * with real-time data, it:
     * <ol>
     *   <li>Retrieves all theoretical trips scheduled for today</li>
     *   <li>Compares with trips observed in real-time data</li>
     *   <li>Marks trips as CANCELED if they start before the latest observed trip
     *       but are not present in real-time data</li>
     * </ol>
     * 
     * @param feedMessage the GTFS-RT feed message builder to append canceled trips
     * @param context processing context containing real-time trip statistics
     */
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

    /**
     * Builds a GTFS-RT entity for a canceled trip.
     * 
     * @param meta the trip metadata from the theoretical schedule
     * @return a FeedEntity with CANCELED schedule relationship
     */
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

    /**
     * Processes a single EstimatedVehicleJourney entity from SIRI Lite data.
     * 
     * <p>This method performs the core trip matching and entity building:
     * <ol>
     *   <li>Extracts line, vehicle, destination, and direction information</li>
     *   <li>Builds a list of estimated calls (stop predictions)</li>
     *   <li>Matches the real-time data to a theoretical GTFS trip</li>
     *   <li>Updates trip state and vehicle associations</li>
     *   <li>Builds a GTFS-RT TripUpdate entity with stop time predictions</li>
     *   <li>Handles canceled trips and skipped stops</li>
     * </ol>
     * 
     * @param entity the SIRI Lite EstimatedVehicleJourney JSON node
     * @param index the original position in the input list (for ordering)
     * @param entitiesTrips optional map for debug output
     * @param context processing context for tracking statistics
     * @return an IndexedEntity containing the built GTFS-RT entity, or null if processing fails
     */
    private IndexedEntity processEntity(JsonNode entity, int index, Map<String, JsonNode> entitiesTrips, ProcessingContext context) {
        String lineId = "IDFM:" + entity.get("LineRef").get("value").asText().split(":")[3];
        String vehicleId = entity.get("DatedVehicleJourneyRef").get("value").asText();

        // Determine direction from SIRI Lite data
        Integer directionIdForMatching = null;
        String journeyNote = null;
        boolean journeyNoteDetailled = false;
        if (
            entity.get("JourneyNote") != null &&
            entity.get("JourneyNote").size() > 0 &&
            entity.get("JourneyNote").get(0).get("value") != null &&
            entity.get("JourneyNote").get(0).get("value").asText().matches("^[A-Z]{4}$") // Exclude 4-letter codes
        ) {
            // Get journey note
            journeyNote = entity.get("JourneyNote").get(0).get("value").asText();
        } else {
            // Get direction from DirectionRef or DirectionName
            int direction = determineDirection(entity);
            directionIdForMatching = (direction != -1) ? direction : null;
        }

        // Specific cases with RATP lines where JourneyNote is detailed
        if (vehicleId.matches("(?<=RATP\\.)[A-Z0-9]+(?=:|$)")) { // RATP RER vehicles
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(?<=RATP\\.)[A-Z0-9]+(?=:)").matcher(vehicleId);
            if (matcher.find()) {
                journeyNote = matcher.group();
            } else {
                journeyNote = vehicleId;
            }
            journeyNoteDetailled = true;
        }

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

        // Find the trip ID using the TripFinder utility
        final String tripId;
        String tmpTripId = null;
        try {
            tmpTripId = TripFinder.findTripIdFromEstimatedCalls(lineId, estimatedCallList, isArrivalTime, destinationId, journeyNote, journeyNoteDetailled, directionIdForMatching);
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

    /**
     * Determines the direction ID from SIRI Lite entity data.
     * 
     * <p>Attempts to extract direction from:
     * <ul>
     *   <li>DirectionRef field (parsing IDFM format codes)</li>
     *   <li>DirectionName field (French and English labels)</li>
     * </ul>
     * 
     * <p>Direction mapping:
     * <ul>
     *   <li>0: Outbound/Retour (R)</li>
     *   <li>1: Inbound/Aller (A)</li>
     * </ul>
     * 
     * @param entity the SIRI Lite EstimatedVehicleJourney JSON node
     * @return 0 or 1 for valid directions, -1 if direction cannot be determined
     */
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

    /**
     * Validates that a stop point reference contains a valid integer ID.
     * 
     * <p>IDFM stop IDs should be numeric after the colon separators.
     * 
     * @param entity the JSON node containing StopPointRef
     * @return true if the stop ID is a valid integer, false otherwise
     */
    boolean checkStopIntegrity(JsonNode entity) {
        // Check if the stop is a integer
        if (entity.has("StopPointRef") && entity.get("StopPointRef").has("value")) {
            String stopPointRef = entity.get("StopPointRef").get("value").asText().split(":")[3];
            return stopPointRef.matches("\\d+"); // Check if the stop ID is an integer
        }

        return false; // Default to false if the check fails
    }

    /**
     * Parses an ISO 8601 timestamp string and converts it to epoch seconds.
     * 
     * <p>Uses a cache to avoid repeated parsing of the same timestamps,
     * which significantly improves performance when processing many entities
     * with recurring time values.
     * 
     * @param timeStr the ISO 8601 timestamp string
     * @return epoch seconds in the Paris timezone
     */
    private long parseTime(String timeStr) {
        // Use the cache to avoid parsing the same timestamp multiple times
        return parsedTimeCache.computeIfAbsent(timeStr, ts -> {
            java.time.Instant instant = java.time.Instant.parse(ts);
            return instant.atZone(ZONE_ID).toEpochSecond();
        });
    }

    /**
     * Extracts and sorts estimated calls from a SIRI Lite vehicle journey.
     * 
     * <p>Estimated calls are sorted by time to ensure they appear in chronological
     * order, which is required for GTFS-RT stop time updates. The method checks
     * multiple time fields in order of preference:
     * <ol>
     *   <li>ExpectedArrivalTime</li>
     *   <li>ExpectedDepartureTime</li>
     *   <li>AimedArrivalTime</li>
     *   <li>AimedDepartureTime</li>
     * </ol>
     * 
     * @param entity the SIRI Lite EstimatedVehicleJourney JSON node
     * @return a sorted list of EstimatedCall JSON nodes
     */
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

    /**
     * Processes a single estimated call and adds it to the trip update.
     * 
     * <p>This method:
     * <ul>
     *   <li>Filters out past stop times (before current time)</li>
     *   <li>Resolves stop IDs and finds the correct stop sequence</li>
     *   <li>Extracts arrival and departure predictions</li>
     *   <li>Handles skipped stops (marked as CANCELLED in SIRI Lite)</li>
     * </ul>
     * 
     * <p><strong>Note:</strong> Stop time updates are added in the order processed.
     * Sorting is performed once at the end of processing all EstimatedCalls to ensure
     * chronological order in the GTFS-RT feed.
     * 
     * @param estimatedCall the SIRI Lite EstimatedCall JSON node
     * @param tripUpdate the GTFS-RT TripUpdate builder to add stop time update
     * @param tripId the GTFS trip ID for stop sequence lookup
     * @param stopTimeUpdates list tracking which stop sequences have been processed
     */
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
    }

    /**
     * Collects results from completed futures and updates the progress bar.
     * 
     * <p>This method iterates through the list of futures, retrieves their results,
     * and filters out null or empty entities. It also handles exceptions that may
     * occur during result retrieval and updates the progress bar after each entity
     * is processed.
     * 
     * @param futures the list of futures containing indexed entities
     * @param total the total number of entities being processed (for progress bar)
     * @return a list of successfully processed indexed entities
     * @throws InterruptedException if the thread is interrupted while waiting for results
     * @throws ExecutionException if an entity processing task threw an exception
     */
    private List<IndexedEntity> collectFutureResults(List<Future<IndexedEntity>> futures, int total) 
            throws InterruptedException, ExecutionException {
        List<IndexedEntity> builtEntities = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            try {
                IndexedEntity result = futures.get(i).get();
                if (result != null && result.entity() != null) {
                    builtEntities.add(result);
                }
            } catch (InterruptedException e) {
                logger.error("Thread interrupted while processing entity index {}: {}", i, e.getMessage(), e);
                Thread.currentThread().interrupt(); // Restore interrupt status
            } catch (ExecutionException e) {
                logger.error("Execution failed for entity index {}: {}", i, e.getMessage(), e);
            }
            renderProgressBar(i + 1, total);
        }
        return builtEntities;
    }

    /**
     * Shuts down the executor service gracefully with timeout handling.
     * 
     * <p>This method performs a graceful shutdown of the executor service:
     * <ol>
     *   <li>Initiates an orderly shutdown</li>
     *   <li>Waits up to 2 minutes for tasks to complete</li>
     *   <li>Forces shutdown if tasks don't complete in time</li>
     *   <li>Waits an additional 1 minute for forced shutdown</li>
     *   <li>Handles interruptions by forcing shutdown and restoring interrupt status</li>
     * </ol>
     * 
     * @param executor the executor service to shut down
     */
    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.MINUTES)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    logger.error("ExecutorService did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Renders a progress bar in the console to track entity processing.
     * 
     * <p>Displays a text-based progress bar with percentage and count.
     * The progress bar is updated in-place using carriage return.
     * 
     * @param current the number of entities processed so far
     * @param total the total number of entities to process
     */
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

    /**
     * Writes the completed GTFS-RT feed to a Protocol Buffer file.
     * 
     * @param feedMessage the GTFS-RT feed message builder containing all entities
     * @param filePath the output file path for the .pb file
     */
    private void writeFeedToFile(GtfsRealtime.FeedMessage.Builder feedMessage, String filePath) {
        try (java.io.FileOutputStream outputStream = new java.io.FileOutputStream(filePath)) {
            feedMessage.build().writeTo(outputStream);
            System.out.println("GTFS-RT feed written to " + filePath);
        } catch (java.io.IOException e) {
            logger.error("Error writing GTFS-RT feed: {}", e.getMessage(), e);
        }
    }
}