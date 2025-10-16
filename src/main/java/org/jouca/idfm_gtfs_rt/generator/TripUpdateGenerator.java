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
    
    /** SIRI Lite JSON field names for time attributes */
    private static final String FIELD_EXPECTED_ARRIVAL_TIME = "ExpectedArrivalTime";
    private static final String FIELD_EXPECTED_DEPARTURE_TIME = "ExpectedDepartureTime";
    private static final String FIELD_AIMED_ARRIVAL_TIME = "AimedArrivalTime";
    private static final String FIELD_AIMED_DEPARTURE_TIME = "AimedDepartureTime";
    
    /** SIRI Lite JSON field name for journey note */
    private static final String FIELD_JOURNEY_NOTE = "JourneyNote";
    
    /** SIRI Lite JSON field name for stop point reference */
    private static final String FIELD_STOP_POINT_REF = "StopPointRef";
    
    /** SIRI Lite JSON field name for departure status */
    private static final String FIELD_DEPARTURE_STATUS = "DepartureStatus";
    
    /** SIRI Lite JSON field name for arrival status */
    private static final String FIELD_ARRIVAL_STATUS = "ArrivalStatus";
    
    /** SIRI Lite JSON field name for direction reference */
    private static final String FIELD_DIRECTION_REF = "DirectionRef";
    
    /** SIRI Lite JSON field name for direction name */
    private static final String FIELD_DIRECTION_NAME = "DirectionName";
    
    /** Common JSON field name used across SIRI Lite responses */
    private static final String FIELD_VALUE = "value";
    
    /** Status value indicating a cancelled stop or trip in SIRI Lite data */
    private static final String STATUS_CANCELLED = "CANCELLED";

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
        List<JsonNode> entities = extractEntitiesFromSiriLite(siriLiteData);
        System.out.println(entities.size() + " entities found in SiriLite data.");
    
        sortEntitiesByTime(entities);
        System.out.println("Processing " + entities.size() + " entities...");

        Map<String, JsonNode> entitiesTrips = dumpDebugFiles ? new ConcurrentHashMap<>() : null;
        List<IndexedEntity> builtEntities = processEntitiesInParallel(entities, entitiesTrips, context);
        
        addEntitiesToFeed(builtEntities, feedMessage);
        cleanupStaleTripStates();
        
        System.out.println("Total trips in GTFS-RT feed: " + feedMessage.getEntityCount());
        exportDebugData(entitiesTrips);
    }

    /**
     * Extracts EstimatedVehicleJourney entities from SIRI Lite data.
     * 
     * @param siriLiteData the JSON data from SIRI Lite API
     * @return list of extracted entities
     */
    private List<JsonNode> extractEntitiesFromSiriLite(JsonNode siriLiteData) {
        List<JsonNode> entities = new ArrayList<>();
        siriLiteData.get("Siri").get("ServiceDelivery").get("EstimatedTimetableDelivery").get(0)
                .get("EstimatedJourneyVersionFrame").get(0).get("EstimatedVehicleJourney").forEach(entities::add);
        return entities;
    }

    /**
     * Sorts entities by their earliest departure or arrival time.
     * 
     * @param entities the list of entities to sort (modified in place)
     */
    private void sortEntitiesByTime(List<JsonNode> entities) {
        entities.sort(Comparator.comparingLong(this::extractFirstCallTime));
    }

    /**
     * Extracts the earliest time from an entity's first estimated call.
     * 
     * @param entity the SIRI Lite entity
     * @return epoch seconds of the earliest time, or Long.MAX_VALUE if no time available
     */
    private long extractFirstCallTime(JsonNode entity) {
        JsonNode estimatedCalls = entity.get("EstimatedCalls").get("EstimatedCall");
        if (estimatedCalls != null && estimatedCalls.size() > 0) {
            JsonNode firstCall = estimatedCalls.get(0);
            String time = null;
            if (firstCall.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
                time = firstCall.get(FIELD_EXPECTED_DEPARTURE_TIME).asText();
            } else if (firstCall.has(FIELD_EXPECTED_ARRIVAL_TIME)) {
                time = firstCall.get(FIELD_EXPECTED_ARRIVAL_TIME).asText();
            } else if (firstCall.has(FIELD_AIMED_DEPARTURE_TIME)) {
                time = firstCall.get(FIELD_AIMED_DEPARTURE_TIME).asText();
            } else if (firstCall.has(FIELD_AIMED_ARRIVAL_TIME)) {
                time = firstCall.get(FIELD_AIMED_ARRIVAL_TIME).asText();
            }
            if (time != null) {
                return Instant.parse(time)
                    .atZone(ZONE_ID)
                    .toLocalDateTime()
                    .atZone(ZONE_ID)
                    .toEpochSecond();
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * Processes entities in parallel using a thread pool.
     * 
     * @param entities the list of entities to process
     * @param entitiesTrips optional map for debug output
     * @param context processing context for tracking statistics
     * @return list of successfully processed indexed entities
     */
    private List<IndexedEntity> processEntitiesInParallel(List<JsonNode> entities, Map<String, JsonNode> entitiesTrips, ProcessingContext context) {
        int total = entities.size();
        renderProgressBar(0, total);

        ExecutorService executor = Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
        List<IndexedEntity> builtEntities = new ArrayList<>();
        try {
            List<Future<IndexedEntity>> futures = submitEntityProcessingTasks(entities, entitiesTrips, context, executor);
            builtEntities = collectFutureResults(futures, total);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted during parallel processing of {} SIRI Lite entities: {}", total, e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.error("Execution error during parallel processing of {} SIRI Lite entities: {}", total, e.getMessage(), e);
        } finally {
            shutdownExecutor(executor);
        }
        return builtEntities;
    }

    /**
     * Submits entity processing tasks to the executor.
     * 
     * @param entities the list of entities to process
     * @param entitiesTrips optional map for debug output
     * @param context processing context
     * @param executor the executor service
     * @return list of futures for the submitted tasks
     */
    private List<Future<IndexedEntity>> submitEntityProcessingTasks(List<JsonNode> entities, Map<String, JsonNode> entitiesTrips, ProcessingContext context, ExecutorService executor) {
        List<Future<IndexedEntity>> futures = new ArrayList<>(entities.size());
        for (int idx = 0; idx < entities.size(); idx++) {
            final int index = idx;
            final JsonNode entity = entities.get(idx);
            futures.add(executor.submit((Callable<IndexedEntity>) () -> processEntity(entity, index, entitiesTrips, context)));
        }
        return futures;
    }

    /**
     * Adds processed entities to the GTFS-RT feed in sorted order.
     * 
     * @param builtEntities the list of indexed entities
     * @param feedMessage the feed message builder
     */
    private void addEntitiesToFeed(List<IndexedEntity> builtEntities, GtfsRealtime.FeedMessage.Builder feedMessage) {
        builtEntities.stream()
            .sorted(Comparator.comparingInt(IndexedEntity::index))
            .forEach(indexed -> feedMessage.addEntity(indexed.entity()));
    }

    /**
     * Cleans up trip states that are older than 15 minutes.
     */
    private void cleanupStaleTripStates() {
        long currentTime = Instant.now().atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();
        tripStates.entrySet().removeIf(entry -> currentTime - entry.getValue().lastUpdate > 15 * 60);
        vehicleToTrip.entrySet().removeIf(e -> !tripStates.containsKey(e.getValue()));
    }

    /**
     * Exports debug data to a JSON file if enabled.
     * 
     * @param entitiesTrips map of trip IDs to their entities
     */
    private void exportDebugData(Map<String, JsonNode> entitiesTrips) {
        if (!dumpDebugFiles || entitiesTrips == null) {
            return;
        }
        
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
        if (!isValidContext(context)) {
            return;
        }

        List<String> routeIds = new ArrayList<>(context.statsByRouteDirection.keySet());
        List<TripFinder.TripMeta> theoreticalTrips = TripFinder.getActiveTripsForRoutesToday(routeIds);
        if (theoreticalTrips == null || theoreticalTrips.isEmpty()) {
            return;
        }

        Map<String, Map<Integer, List<TripFinder.TripMeta>>> theoreticalByRouteDirection = 
                groupTheoreticalTripsByRouteAndDirection(theoreticalTrips);

        Set<String> existingEntityIds = extractExistingEntityIds(feedMessage);

        for (String routeId : routeIds) {
            processCanceledTripsForRoute(routeId, context, theoreticalByRouteDirection, existingEntityIds, feedMessage);
        }
    }

    /**
     * Validates that the processing context is not null and contains data.
     * 
     * @param context the processing context to validate
     * @return true if context is valid, false otherwise
     */
    private boolean isValidContext(ProcessingContext context) {
        return context != null && !context.statsByRouteDirection.isEmpty();
    }

    /**
     * Groups theoretical trips by route ID and direction ID.
     * 
     * @param theoreticalTrips the list of theoretical trips to group
     * @return map of trips grouped by route and direction
     */
    private Map<String, Map<Integer, List<TripFinder.TripMeta>>> groupTheoreticalTripsByRouteAndDirection(
            List<TripFinder.TripMeta> theoreticalTrips) {
        return theoreticalTrips.stream()
                .collect(Collectors.groupingBy(meta -> meta.routeId,
                        Collectors.groupingBy(meta -> meta.directionId)));
    }

    /**
     * Extracts existing entity IDs from the feed message.
     * 
     * @param feedMessage the feed message to extract IDs from
     * @return set of existing entity IDs
     */
    private Set<String> extractExistingEntityIds(GtfsRealtime.FeedMessage.Builder feedMessage) {
        return feedMessage.getEntityList().stream()
                .map(GtfsRealtime.FeedEntity::getId)
                .collect(Collectors.toCollection(java.util.HashSet::new));
    }

    /**
     * Processes canceled trips for a specific route.
     * 
     * @param routeId the route ID to process
     * @param context the processing context
     * @param theoreticalByRouteDirection grouped theoretical trips
     * @param existingEntityIds set of existing entity IDs
     * @param feedMessage the feed message builder to append entities
     */
    private void processCanceledTripsForRoute(String routeId, ProcessingContext context,
            Map<String, Map<Integer, List<TripFinder.TripMeta>>> theoreticalByRouteDirection,
            Set<String> existingEntityIds, GtfsRealtime.FeedMessage.Builder feedMessage) {
        
        Map<Integer, RealtimeDirectionStats> statsByDirection = context.statsByRouteDirection.get(routeId);
        if (statsByDirection == null || statsByDirection.isEmpty()) {
            return;
        }

        Map<Integer, List<TripFinder.TripMeta>> theoreticalByDirection = theoreticalByRouteDirection.get(routeId);
        if (theoreticalByDirection == null || theoreticalByDirection.isEmpty()) {
            return;
        }

        for (Map.Entry<Integer, RealtimeDirectionStats> entry : statsByDirection.entrySet()) {
            processCanceledTripsForDirection(entry.getKey(), entry.getValue(), 
                    theoreticalByDirection, existingEntityIds, feedMessage);
        }
    }

    /**
     * Processes canceled trips for a specific direction.
     * 
     * @param directionId the direction ID
     * @param stats the real-time direction statistics
     * @param theoreticalByDirection theoretical trips grouped by direction
     * @param existingEntityIds set of existing entity IDs
     * @param feedMessage the feed message builder to append entities
     */
    private void processCanceledTripsForDirection(int directionId, RealtimeDirectionStats stats,
            Map<Integer, List<TripFinder.TripMeta>> theoreticalByDirection,
            Set<String> existingEntityIds, GtfsRealtime.FeedMessage.Builder feedMessage) {
        
        if (!isValidDirectionStats(stats)) {
            return;
        }

        List<TripFinder.TripMeta> candidates = theoreticalByDirection.get(directionId);
        if (candidates == null || candidates.isEmpty()) {
            return;
        }

        long cutoff = stats.maxStartTime;
        if (cutoff == Long.MIN_VALUE) {
            return;
        }

        addCanceledTripEntities(candidates, cutoff, stats.tripIds, existingEntityIds, feedMessage);
    }

    /**
     * Validates that direction statistics contain valid data.
     * 
     * @param stats the direction statistics to validate
     * @return true if valid, false otherwise
     */
    private boolean isValidDirectionStats(RealtimeDirectionStats stats) {
        return stats != null && !stats.tripIds.isEmpty();
    }

    /**
     * Filters and adds canceled trip entities to the feed.
     * 
     * @param candidates list of candidate trips to check
     * @param cutoff the maximum start time cutoff
     * @param realtimeTripIds set of trip IDs present in real-time data
     * @param existingEntityIds set of existing entity IDs
     * @param feedMessage the feed message builder to append entities
     */
    private void addCanceledTripEntities(List<TripFinder.TripMeta> candidates, long cutoff,
            Set<String> realtimeTripIds, Set<String> existingEntityIds,
            GtfsRealtime.FeedMessage.Builder feedMessage) {
        
        candidates.stream()
                .sorted(Comparator.comparingInt(meta -> meta.firstTimeSecOfDay))
                .filter(meta -> isTripCanceled(meta, cutoff, realtimeTripIds))
                .filter(meta -> existingEntityIds.add(meta.tripId))
                .forEach(meta -> feedMessage.addEntity(buildCanceledEntity(meta)));
    }

    /**
     * Determines if a trip should be marked as canceled.
     * 
     * @param meta the trip metadata
     * @param cutoff the time cutoff
     * @param realtimeTripIds set of trip IDs in real-time data
     * @return true if the trip should be canceled, false otherwise
     */
    private boolean isTripCanceled(TripFinder.TripMeta meta, long cutoff, Set<String> realtimeTripIds) {
        return meta.firstTimeSecOfDay <= cutoff && !realtimeTripIds.contains(meta.tripId);
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
        String lineId = "IDFM:" + entity.get("LineRef").get(FIELD_VALUE).asText().split(":")[3];
        String vehicleId = entity.get("DatedVehicleJourneyRef").get(FIELD_VALUE).asText();

        DirectionInfo directionInfo = extractDirectionInfo(entity, vehicleId);
        
        String destinationId = extractDestinationId(entity);
        if (destinationId == null) return null;

        List<JsonNode> estimatedCalls = getSortedEstimatedCalls(entity);
        List<org.jouca.idfm_gtfs_rt.records.EstimatedCall> estimatedCallList = buildEstimatedCallList(estimatedCalls);

        if (lineId.isEmpty() || estimatedCallList.isEmpty()) {
            return null;
        }

        String tripId = findTripId(lineId, estimatedCallList, estimatedCalls, destinationId, directionInfo);
        if (tripId == null || tripId.isEmpty()) {
            return null;
        }

        TripFinder.TripMeta tripMeta = TripFinder.getTripMeta(tripId);
        updateContextStats(context, tripMeta);

        TripState state = updateTripState(tripId, vehicleId);

        GtfsRealtime.FeedEntity feedEntity = buildFeedEntity(tripId, state, tripMeta, directionInfo.directionIdForMatching(), 
                lineId, estimatedCalls);

        if (entitiesTrips != null) {
            entitiesTrips.put(tripId, entity);
        }
        return new IndexedEntity(index, feedEntity);
    }

    /**
     * Record to hold direction and journey note information extracted from an entity.
     */
    private record DirectionInfo(Integer directionIdForMatching, String journeyNote, boolean journeyNoteDetailled) {}

    /**
     * Extracts direction and journey note information from SIRI Lite entity.
     * 
     * @param entity the SIRI Lite entity
     * @param vehicleId the vehicle identifier
     * @return DirectionInfo containing direction ID, journey note, and detail flag
     */
    private DirectionInfo extractDirectionInfo(JsonNode entity, String vehicleId) {
        Integer directionIdForMatching = null;
        String journeyNote = null;
        boolean journeyNoteDetailled = false;

        if (entity.get(FIELD_JOURNEY_NOTE) != null &&
            entity.get(FIELD_JOURNEY_NOTE).size() > 0 &&
            entity.get(FIELD_JOURNEY_NOTE).get(0).get(FIELD_VALUE) != null &&
            entity.get(FIELD_JOURNEY_NOTE).get(0).get(FIELD_VALUE).asText().matches("^[A-Z]{4}$")) {
            journeyNote = entity.get(FIELD_JOURNEY_NOTE).get(0).get(FIELD_VALUE).asText();
        } else {
            int direction = determineDirection(entity);
            directionIdForMatching = (direction != -1) ? direction : null;
        }

        if (vehicleId.matches("(?<=RATP\\.)[A-Z0-9]+(?=:|$)")) {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(?<=RATP\\.)[A-Z0-9]+(?=:)").matcher(vehicleId);
            if (matcher.find()) {
                journeyNote = matcher.group();
            } else {
                journeyNote = vehicleId;
            }
            journeyNoteDetailled = true;
        }

        return new DirectionInfo(directionIdForMatching, journeyNote, journeyNoteDetailled);
    }

    /**
     * Extracts and resolves the destination ID from SIRI Lite entity.
     * 
     * @param entity the SIRI Lite entity
     * @return the resolved destination ID, or null if not found
     */
    private String extractDestinationId(JsonNode entity) {
        String destinationIdCode = entity.get("DestinationRef").get(FIELD_VALUE).asText().split(":")[3];
        return TripFinder.resolveStopId(destinationIdCode);
    }

    /**
     * Builds a list of EstimatedCall records from SIRI Lite estimated calls.
     * 
     * @param estimatedCalls the list of estimated call JSON nodes
     * @return list of EstimatedCall records
     */
    private List<org.jouca.idfm_gtfs_rt.records.EstimatedCall> buildEstimatedCallList(List<JsonNode> estimatedCalls) {
        List<org.jouca.idfm_gtfs_rt.records.EstimatedCall> estimatedCallList = new ArrayList<>();
        for (JsonNode call : estimatedCalls) {
            String stopCode = call.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3];
            String stopId = TripFinder.resolveStopId(stopCode);
            if (stopId == null) continue;

            String isoTime = extractTimeFromCall(call);
            if (isoTime != null) {
                estimatedCallList.add(new org.jouca.idfm_gtfs_rt.records.EstimatedCall(stopId, isoTime));
            }
        }
        return estimatedCallList;
    }

    /**
     * Extracts the first available time value from an estimated call.
     * 
     * @param call the estimated call JSON node
     * @return ISO time string, or null if no time found
     */
    private String extractTimeFromCall(JsonNode call) {
        if (call.has(FIELD_EXPECTED_ARRIVAL_TIME)) {
            return call.get(FIELD_EXPECTED_ARRIVAL_TIME).asText();
        } else if (call.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
            return call.get(FIELD_EXPECTED_DEPARTURE_TIME).asText();
        } else if (call.has(FIELD_AIMED_ARRIVAL_TIME)) {
            return call.get(FIELD_AIMED_ARRIVAL_TIME).asText();
        } else if (call.has(FIELD_AIMED_DEPARTURE_TIME)) {
            return call.get(FIELD_AIMED_DEPARTURE_TIME).asText();
        }
        return null;
    }

    /**
     * Finds the GTFS trip ID that matches the real-time data.
     * 
     * @param lineId the line identifier
     * @param estimatedCallList the list of estimated calls
     * @param estimatedCalls the original JSON nodes
     * @param destinationId the destination stop ID
     * @param directionInfo the direction information
     * @return the matched trip ID, or null if not found
     */
    private String findTripId(String lineId, List<org.jouca.idfm_gtfs_rt.records.EstimatedCall> estimatedCallList,
                              List<JsonNode> estimatedCalls, String destinationId, DirectionInfo directionInfo) {
        boolean isArrivalTime = !estimatedCallList.isEmpty() && 
                (estimatedCalls.get(0).has(FIELD_EXPECTED_ARRIVAL_TIME) || 
                 estimatedCalls.get(0).has(FIELD_AIMED_ARRIVAL_TIME));

        try {
            return TripFinder.findTripIdFromEstimatedCalls(lineId, estimatedCallList, isArrivalTime, 
                    destinationId, directionInfo.journeyNote(), directionInfo.journeyNoteDetailled(), 
                    directionInfo.directionIdForMatching());
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Updates the processing context statistics with trip metadata.
     * 
     * @param context the processing context
     * @param tripMeta the trip metadata
     */
    private void updateContextStats(ProcessingContext context, TripFinder.TripMeta tripMeta) {
        if (context != null && tripMeta != null && tripMeta.routeId != null) {
            context.statsByRouteDirection
                    .computeIfAbsent(tripMeta.routeId, r -> new ConcurrentHashMap<>())
                    .computeIfAbsent(tripMeta.directionId, d -> new RealtimeDirectionStats())
                    .addTrip(tripMeta);
        }
    }

    /**
     * Updates trip state and vehicle associations.
     * 
     * @param tripId the trip identifier
     * @param vehicleId the vehicle identifier
     * @return the updated trip state
     */
    private TripState updateTripState(String tripId, String vehicleId) {
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
        
        return state;
    }

    /**
     * Builds a GTFS-RT FeedEntity from processed trip data.
     * 
     * @param tripId the trip identifier
     * @param state the trip state
     * @param tripMeta the trip metadata
     * @param directionIdForMatching the direction ID from SIRI Lite
     * @param lineId the line identifier
     * @param estimatedCalls the list of estimated calls
     * @return the built FeedEntity
     */
    private GtfsRealtime.FeedEntity buildFeedEntity(String tripId, TripState state, TripFinder.TripMeta tripMeta,
                                                     Integer directionIdForMatching, String lineId, 
                                                     List<JsonNode> estimatedCalls) {
        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        int directionId = resolveDirectionId(tripMeta, directionIdForMatching, tripId);
        String routeForDescriptor = tripMeta != null && tripMeta.routeId != null ? tripMeta.routeId : lineId;
        
        tripUpdate.getTripBuilder()
                .setRouteId(routeForDescriptor)
                .setDirectionId(directionId)
                .setTripId(tripId);
        tripUpdate.getVehicleBuilder().setId(state.vehicleId);

        addStopTimeUpdates(tripUpdate, estimatedCalls, tripId);
        
        return entityBuilder.build();
    }

    /**
     * Resolves the direction ID from available sources.
     * 
     * @param tripMeta the trip metadata
     * @param directionIdForMatching the direction from SIRI Lite
     * @param tripId the trip identifier
     * @return the resolved direction ID
     */
    private int resolveDirectionId(TripFinder.TripMeta tripMeta, Integer directionIdForMatching, String tripId) {
        if (tripMeta != null) {
            return tripMeta.directionId;
        } else if (directionIdForMatching != null) {
            return directionIdForMatching;
        } else {
            String directionStr = TripFinder.getTripDirection(tripId);
            if (directionStr != null && !directionStr.isEmpty()) {
                try {
                    return Integer.parseInt(directionStr);
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return 0;
    }

    /**
     * Adds stop time updates to the trip update, or marks the trip as canceled.
     * 
     * @param tripUpdate the trip update builder
     * @param estimatedCalls the list of estimated calls
     * @param tripId the trip identifier
     */
    private void addStopTimeUpdates(GtfsRealtime.TripUpdate.Builder tripUpdate, List<JsonNode> estimatedCalls, String tripId) {
        boolean allCancelled = estimatedCalls.stream().allMatch(call ->
            (call.has(FIELD_DEPARTURE_STATUS) && call.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_CANCELLED)) ||
            (call.has(FIELD_ARRIVAL_STATUS) && call.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_CANCELLED))
        );
        
        if (allCancelled) {
            tripUpdate.getTripBuilder().setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        } else {
            List<String> stopTimeUpdates = new ArrayList<>();
            for (JsonNode estimatedCall : estimatedCalls) {
                processEstimatedCall(estimatedCall, tripUpdate, tripId, stopTimeUpdates);
            }
        }
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
        // Try DirectionRef field first
        int direction = parseDirectionFromRef(entity);
        if (direction != -1) {
            return direction;
        }
        
        // Fall back to DirectionName field
        return parseDirectionFromName(entity);
    }

    /**
     * Parses direction from the DirectionRef field.
     * 
     * @param entity the SIRI Lite entity
     * @return direction ID (0 or 1), or -1 if not found or invalid
     */
    private int parseDirectionFromRef(JsonNode entity) {
        if (!entity.has(FIELD_DIRECTION_REF) || !entity.get(FIELD_DIRECTION_REF).has(FIELD_VALUE)) {
            return -1;
        }
        
        String directionValue = entity.get(FIELD_DIRECTION_REF).get(FIELD_VALUE).asText();
        
        if (directionValue.contains(":")) {
            return parseDirectionFromColonDelimitedValue(directionValue);
        }
        
        return parseDirectionFromSimpleValue(directionValue);
    }

    /**
     * Parses direction from a colon-delimited DirectionRef value (IDFM format).
     * 
     * @param directionValue the colon-delimited direction value
     * @return direction ID (0 or 1), or -1 if not found or invalid
     */
    private int parseDirectionFromColonDelimitedValue(String directionValue) {
        String[] directionParts = directionValue.split(":");
        if (directionParts.length <= 3) {
            return -1;
        }
        
        String directionString = directionParts[3];
        if ("A".equals(directionString)) {
            return 1;
        } else if ("R".equals(directionString)) {
            return 0;
        }
        
        return -1;
    }

    /**
     * Parses direction from a simple (non-colon-delimited) direction value.
     * 
     * @param directionValue the direction value to parse
     * @return direction ID (0 or 1), or -1 if not found or invalid
     */
    private int parseDirectionFromSimpleValue(String directionValue) {
        if ("Aller".equals(directionValue) || "inbound".equals(directionValue) || "A".equals(directionValue)) {
            return 1;
        } else if ("Retour".equals(directionValue) || "outbound".equals(directionValue) || "R".equals(directionValue)) {
            return 0;
        }
        
        return -1;
    }

    /**
     * Parses direction from the DirectionName field.
     * 
     * @param entity the SIRI Lite entity
     * @return direction ID (0 or 1), or -1 if not found or invalid
     */
    private int parseDirectionFromName(JsonNode entity) {
        if (!entity.has(FIELD_DIRECTION_NAME) || entity.get(FIELD_DIRECTION_NAME).size() == 0) {
            return -1;
        }
        
        String directionName = entity.get(FIELD_DIRECTION_NAME).get(0).get(FIELD_VALUE).asText();
        
        // IDFM specific cases (single letter)
        if ("A".equals(directionName)) {
            return 0;
        } else if ("R".equals(directionName)) {
            return 1;
        }
        
        // French and English labels
        if ("Aller".equals(directionName) || "inbound".equals(directionName)) {
            return 1;
        } else if ("Retour".equals(directionName) || "outbound".equals(directionName)) {
            return 0;
        }
        
        return -1;
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
        if (entity.has(FIELD_STOP_POINT_REF) && entity.get(FIELD_STOP_POINT_REF).has(FIELD_VALUE)) {
            String stopPointRef = entity.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3];
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
                .sorted(Comparator.comparingLong(this::extractCallTimeForSorting))
                .collect(Collectors.toList());
    }

    /**
     * Extracts the time value from an estimated call for sorting purposes.
     * 
     * <p>Checks time fields in order of preference and returns the epoch seconds
     * of the first available time, or Long.MAX_VALUE if no time is found.
     * 
     * @param call the estimated call JSON node
     * @return epoch seconds of the call time, or Long.MAX_VALUE if unavailable
     */
    private long extractCallTimeForSorting(JsonNode call) {
        String callTime = null;
        
        if (call.has(FIELD_EXPECTED_ARRIVAL_TIME)) {
            callTime = call.get(FIELD_EXPECTED_ARRIVAL_TIME).asText();
        } else if (call.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
            callTime = call.get(FIELD_EXPECTED_DEPARTURE_TIME).asText();
        } else if (call.has(FIELD_AIMED_ARRIVAL_TIME)) {
            callTime = call.get(FIELD_AIMED_ARRIVAL_TIME).asText();
        } else if (call.has(FIELD_AIMED_DEPARTURE_TIME)) {
            callTime = call.get(FIELD_AIMED_DEPARTURE_TIME).asText();
        }
        
        if (callTime != null) {
            return Instant.parse(callTime).atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();
        }
        
        return Long.MAX_VALUE;
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
        // Skip if times are in the past
        if (isEstimatedCallInPast(estimatedCall)) {
            return;
        }

        String stopId = TripFinder.resolveStopId(estimatedCall.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3]);
        if (stopId == null) return;

        String stopSequence = TripFinder.findStopSequence(tripId, stopId, stopTimeUpdates);
        if (stopSequence == null) return;

        stopTimeUpdates.add(stopSequence);

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = tripUpdate.addStopTimeUpdateBuilder();
        stopTimeUpdate.setStopSequence(Integer.parseInt(stopSequence));
        stopTimeUpdate.setStopId(stopId);

        // Set arrival and departure times
        setArrivalTime(estimatedCall, stopTimeUpdate);
        setDepartureTime(estimatedCall, stopTimeUpdate);

        // Handle cancellations
        handleCancellationStatus(estimatedCall, stopTimeUpdate);
    }

    /**
     * Checks if the estimated call is in the past and should be skipped.
     * 
     * @param estimatedCall the estimated call to check
     * @return true if the call is in the past, false otherwise
     */
    private boolean isEstimatedCallInPast(JsonNode estimatedCall) {
        // Check arrival times
        if (estimatedCall.has(FIELD_EXPECTED_ARRIVAL_TIME)) {
            long arrivalTime = parseTime(estimatedCall.get(FIELD_EXPECTED_ARRIVAL_TIME).asText());
            if (arrivalTime < currentEpochSecond) return true;
        } else if (estimatedCall.has(FIELD_AIMED_ARRIVAL_TIME)) {
            long arrivalTime = parseTime(estimatedCall.get(FIELD_AIMED_ARRIVAL_TIME).asText());
            if (arrivalTime < currentEpochSecond) return true;
        }

        // Check departure times
        if (estimatedCall.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_EXPECTED_DEPARTURE_TIME).asText());
            if (departureTime < currentEpochSecond) return true;
        } else if (estimatedCall.has(FIELD_AIMED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_AIMED_DEPARTURE_TIME).asText());
            if (departureTime < currentEpochSecond) return true;
        }

        return false;
    }

    /**
     * Sets the arrival time on the stop time update from the estimated call.
     * Prefers ExpectedArrivalTime over AimedArrivalTime.
     * 
     * @param estimatedCall the estimated call containing time data
     * @param stopTimeUpdate the stop time update builder to set the arrival time on
     */
    private void setArrivalTime(JsonNode estimatedCall, GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate) {
        if (estimatedCall.has(FIELD_EXPECTED_ARRIVAL_TIME)) {
            long arrivalTime = parseTime(estimatedCall.get(FIELD_EXPECTED_ARRIVAL_TIME).asText());
            stopTimeUpdate.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrivalTime).build());
        } else if (estimatedCall.has(FIELD_AIMED_ARRIVAL_TIME)) {
            long arrivalTime = parseTime(estimatedCall.get(FIELD_AIMED_ARRIVAL_TIME).asText());
            stopTimeUpdate.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrivalTime).build());
        }
    }

    /**
     * Sets the departure time on the stop time update from the estimated call.
     * Prefers ExpectedDepartureTime over AimedDepartureTime.
     * 
     * @param estimatedCall the estimated call containing time data
     * @param stopTimeUpdate the stop time update builder to set the departure time on
     */
    private void setDepartureTime(JsonNode estimatedCall, GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate) {
        if (estimatedCall.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_EXPECTED_DEPARTURE_TIME).asText());
            stopTimeUpdate.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
        } else if (estimatedCall.has(FIELD_AIMED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_AIMED_DEPARTURE_TIME).asText());
            stopTimeUpdate.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
        }
    }

    /**
     * Handles cancellation status for the stop time update.
     * If either departure or arrival is cancelled, marks the stop as SKIPPED
     * and clears the timing information.
     * 
     * @param estimatedCall the estimated call containing status information
     * @param stopTimeUpdate the stop time update builder to update
     */
    private void handleCancellationStatus(JsonNode estimatedCall, GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate) {
        boolean isDepartureCancelled = estimatedCall.has(FIELD_DEPARTURE_STATUS) && 
                                        estimatedCall.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_CANCELLED);
        boolean isArrivalCancelled = estimatedCall.has(FIELD_ARRIVAL_STATUS) && 
                                      estimatedCall.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_CANCELLED);

        if (isDepartureCancelled || isArrivalCancelled) {
            stopTimeUpdate.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
            stopTimeUpdate.clearArrival();
            stopTimeUpdate.clearDeparture();
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