package org.jouca.idfm_gtfs_rt.finders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.jouca.idfm_gtfs_rt.records.EstimatedCall;

/**
 * TripFinder is responsible for matching real-time transit data to scheduled GTFS trips.
 * 
 * <p>This class provides various methods to:
 * <ul>
 *   <li>Find trip IDs based on real-time estimated calls and scheduled data</li>
 *   <li>Resolve stop IDs from various identifiers (codes, extensions)</li>
 *   <li>Retrieve trip metadata, stop sequences, and scheduled times</li>
 *   <li>Handle midnight-crossing trips (trips that continue past midnight on the same service day)</li>
 * </ul>
 * 
 * <p>The class uses a SQLite database connection pool for efficient database access and implements
 * various caching strategies to minimize database queries during processing.
 * 
 * <p><b>Service Day Handling:</b> GTFS allows times >= 24:00:00 to represent trips continuing past
 * midnight on the same service day. This class determines the appropriate service day by considering:
 * <ul>
 *   <li>The program start time</li>
 *   <li>Whether the current time is in early morning hours (before 3 AM)</li>
 *   <li>The relationship between calendar day and service day</li>
 * </ul>
 * 
 * @author Jouca
 * @since 1.0
 * 
 * @see EstimatedCall
 * @see TripMeta
 */
public class TripFinder {
    /** JDBC connection URL for the SQLite GTFS database */
    private static final String DB_URL = "jdbc:sqlite:./gtfs.db";
    
    /** Column name constant for stop_id to avoid string duplication */
    private static final String COL_STOP_ID = "stop_id";
    
    /** Column name constant for direction_id to avoid string duplication */
    private static final String COL_DIRECTION_ID = "direction_id";
    
    /** Connection pool for database access with optimized settings for concurrent operations */
    private static final BasicDataSource dataSource = new BasicDataSource();
    
    /**
     * The timezone used for all date and time operations in the GTFS system.
     * Paris timezone is used as this is for the Île-de-France Mobilités (IDFM) transit system.
     */
    private static final ZoneId PARIS_ZONE = ZoneId.of("Europe/Paris");
    
    /**
     * Stores the program start time to determine service day consistently.
     * This ensures that a trip running at 00:30 AM is matched against the previous day's service
     * if the program started before midnight.
     */
    private static final ZonedDateTime PROGRAM_START_TIME = ZonedDateTime.now(PARIS_ZONE);
    
    /**
     * Threshold for considering times as potentially belonging to the previous day's service.
     * Times before 3 AM are considered as potentially part of the previous day's service for
     * midnight-crossing trips.
     */
    private static final int MIDNIGHT_CROSSING_THRESHOLD_HOURS = 3;

    /**
     * Static initializer block that configures the database connection pool.
     * Sets up connection pooling parameters and SQLite pragmas for optimal performance:
     * <ul>
     *   <li>WAL (Write-Ahead Logging) mode for better concurrency</li>
     *   <li>NORMAL synchronous mode for better performance with minimal safety trade-off</li>
     *   <li>Memory-based temporary storage</li>
     *   <li>Large cache size for frequently accessed data</li>
     *   <li>Memory-mapped I/O for faster reads</li>
     * </ul>
     */
    static {
        dataSource.setUrl(DB_URL);
        dataSource.setMinIdle(12); // Increase min idle connections
        dataSource.setMaxIdle(36); // Increase max idle connections
        dataSource.setMaxTotal(72); // Allow more total connections
        dataSource.setMaxOpenPreparedStatements(256); // Allow more prepared statements
        dataSource.setInitialSize(12); // Pre-initialize connections
        dataSource.setPoolPreparedStatements(true); // Enable prepared statement pooling
        dataSource.setDefaultQueryTimeout(Duration.ofSeconds(45));
        dataSource.setConnectionInitSqls(Arrays.asList(
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA cache_size=-131072",
            "PRAGMA mmap_size=268435456"
        ));
    }

    /**
     * Lightweight container for trip metadata needed to build GTFS-RT TripDescriptor.
     * 
     * <p>This class holds essential information about a scheduled trip that is needed
     * for creating GTFS Realtime trip descriptors and for matching real-time data to
     * scheduled trips.
     * 
     * @see <a href="https://gtfs.org/realtime/reference/#message-tripdescriptor">GTFS Realtime TripDescriptor</a>
     */
    public static class TripMeta {
        /** The unique identifier for this trip from the GTFS trips.txt file */
        public final String tripId;
        
        /** The route identifier this trip belongs to from the GTFS routes.txt file */
        public final String routeId;
        
        /** 
         * The direction of travel for this trip (typically 0 or 1).
         * Convention is usually 0 for outbound and 1 for inbound, but varies by agency.
         */
        public final int directionId;
        
        /** 
         * First stop time in seconds since start of service day (0-86399 for same-day trips,
         * may be >= 86400 for trips continuing past midnight on the same service day).
         */
        public final int firstTimeSecOfDay;

        /**
         * Constructs a new TripMeta instance.
         * 
         * @param tripId The unique trip identifier
         * @param routeId The route identifier
         * @param directionId The direction of travel (0 or 1)
         * @param firstTimeSecOfDay First stop time in seconds since service day start
         */
        public TripMeta(String tripId, String routeId, int directionId, int firstTimeSecOfDay) {
            this.tripId = tripId;
            this.routeId = routeId;
            this.directionId = directionId;
            this.firstTimeSecOfDay = firstTimeSecOfDay;
        }
    }

    /**
     * Parameter object that encapsulates the query parameters needed for trip finding operations.
     * This helps reduce method parameter counts and groups related data together.
     * 
     * @since 1.0
     */
    public static class TripQueryParameters {
        /** Service day in YYYYMMDD format */
        public final String yyyymmdd;
        
        /** Weekday name (e.g., "monday", "tuesday") for service calendar lookup */
        public final String weekday;
        
        /** Route identifier from GTFS routes.txt */
        public final String routeId;
        
        /** List of all stop identifiers involved in the query */
        public final List<String> allStopIds;
        
        /** Direction ID (0 or 1), may be null if not filtering by direction */
        public final Integer directionId;
        
        /** Destination stop identifier, may be null */
        public final String destinationId;
        
        /** Journey note/pattern identifier, may be null */
        public final String journeyNote;

        /**
         * Constructs a new TripQueryParameters instance.
         * 
         * @param yyyymmdd Service day in YYYYMMDD format
         * @param weekday Weekday name for calendar lookup
         * @param routeId Route identifier
         * @param allStopIds List of stop identifiers
         * @param directionId Direction ID (may be null)
         * @param destinationId Destination stop ID (may be null)
         * @param journeyNote Journey note identifier (may be null)
         */
        public TripQueryParameters(String yyyymmdd, String weekday, String routeId, 
                                  List<String> allStopIds, Integer directionId, 
                                  String destinationId, String journeyNote) {
            this.yyyymmdd = yyyymmdd;
            this.weekday = weekday;
            this.routeId = routeId;
            this.allStopIds = allStopIds;
            this.directionId = directionId;
            this.destinationId = destinationId;
            this.journeyNote = journeyNote;
        }
    }

    /**
     * Determines the service day for a given timestamp, accounting for trips that cross midnight.
     * GTFS allows times >= 24:00:00 to represent trips continuing past midnight on the same service day.
     * 
     * Logic:
     * - If the timestamp is in early morning hours (before MIDNIGHT_CROSSING_THRESHOLD_HOURS, e.g., 3 AM)
     *   AND the program started before midnight, the service day is the previous calendar day.
     * - Otherwise, the service day is the calendar day of the timestamp.
     * 
     * @param timestamp The real-time timestamp to determine service day for
     * @param zone The timezone to use
     * @return The service day (may be different from calendar day for early morning times)
     */
    private static LocalDate determineServiceDay(ZonedDateTime timestamp, ZoneId zone) {
        LocalDate calendarDate = timestamp.toLocalDate();
        int hour = timestamp.getHour();
        
        // If we're in early morning hours (00:00 - 03:00), this might be part of previous day's service
        if (hour < MIDNIGHT_CROSSING_THRESHOLD_HOURS) {
            // Check if program started on the previous day
            LocalDate programStartDate = PROGRAM_START_TIME.toLocalDate();
            
            // If program started yesterday and we're now in early morning of next day,
            // OR if the current time is in early morning, assume it's part of previous day's service
            if (calendarDate.isAfter(programStartDate) || hour < MIDNIGHT_CROSSING_THRESHOLD_HOURS) {
                return calendarDate.minusDays(1);
            }
        }
        
        return calendarDate;
    }

    /**
     * Determines the service day from a list of estimated calls.
     * Returns the service day of the first estimated call.
     */
    private static LocalDate determineServiceDayFromEstimatedCalls(
        List<EstimatedCall> estimatedCalls,
        ZoneId zone
    ) {
        if (estimatedCalls.isEmpty()) {
            return LocalDate.now(zone);
        }
        
        EstimatedCall firstCall = estimatedCalls.get(0);
        Instant instant = Instant.parse(firstCall.isoTime());
        ZonedDateTime zdt = instant.atZone(zone);
        return determineServiceDay(zdt, zone);
    }

    /**
     * Converts an EstimatedCall to seconds since service day start.
     */
    private static int convertToSecondsSinceServiceDay(
        EstimatedCall ec,
        ZoneId zone
    ) {
        Instant inst = Instant.parse(ec.isoTime());
        ZonedDateTime zdtEc = inst.atZone(zone);
        LocalDate serviceDayForEc = determineServiceDay(zdtEc, zone);
        return (int) (zdtEc.toEpochSecond() - serviceDayForEc.atStartOfDay(zone).toEpochSecond());
    }

    /**
     * Builds the SQL query for finding candidate trips.
     */
    private static String buildTripFinderQuery(
        String timeColumn,
        List<String> allStopIds,
        Integer directionId,
        String journeyNote,
        boolean journeyNoteDetailled
    ) {
        StringBuilder query = new StringBuilder("""
            WITH valid_services AS (
                SELECT service_id FROM calendar
                WHERE start_date <= ? AND end_date >= ?
                AND (
                    (monday = 1 AND ? = '1') OR
                    (tuesday = 1 AND ? = '2') OR
                    (wednesday = 1 AND ? = '3') OR
                    (thursday = 1 AND ? = '4') OR
                    (friday = 1 AND ? = '5') OR
                    (saturday = 1 AND ? = '6') OR
                    (sunday = 1 AND ? = '0')
                )
                UNION
                SELECT service_id FROM calendar_dates
                WHERE date = ? AND exception_type = 1
            ),
            excluded_services AS (
                SELECT service_id FROM calendar_dates
                WHERE date = ? AND exception_type = 2
            )
            SELECT st.trip_id, st.stop_id, st.stop_sequence, st.%s as stop_time
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            WHERE t.route_id = ?
            AND t.service_id IN (SELECT service_id FROM valid_services)
            AND t.service_id NOT IN (SELECT service_id FROM excluded_services)
        """.formatted(timeColumn));

        query.append("AND st.stop_id IN (")
             .append(allStopIds.stream().map(x -> "?").collect(Collectors.joining(",")))
             .append(")\n");

        if (directionId != null) {
            query.append("AND t.direction_id = ?\n");
        }

        query.append("""
            AND (
                st.trip_id NOT NULL
                AND EXISTS (
                    SELECT 1 FROM stop_times st2
                    WHERE st2.trip_id = st.trip_id
                    AND st2.stop_id = ?
                    AND st2.stop_sequence = (
                        SELECT MAX(st3.stop_sequence)
                        FROM stop_times st3
                        WHERE st3.trip_id = st.trip_id
                    )
                )
            )
        """);

        if (journeyNote != null) {
            if (journeyNoteDetailled) {
                query.append("AND t.trip_short_name LIKE ?\n");
            } else {
                query.append("AND t.trip_headsign LIKE ?\n");
            }
        }

        query.append("ORDER BY st.departure_timestamp ASC;");
        return query.toString();
    }

    /**
     * Binds parameters to the prepared statement for trip finder query.
     * 
     * @param stmt The prepared statement to bind parameters to
     * @param params The query parameters encapsulated in a TripQueryParameters object
     * @throws SQLException If a database access error occurs
     */
    private static void bindTripFinderParameters(
        PreparedStatement stmt,
        TripQueryParameters params
    ) throws SQLException {
        int i = 1;
        stmt.setString(i++, params.yyyymmdd);
        stmt.setString(i++, params.yyyymmdd);
        for (int j = 0; j < 7; j++) stmt.setString(i++, params.weekday);
        stmt.setString(i++, params.yyyymmdd);
        stmt.setString(i++, params.yyyymmdd);
        stmt.setString(i++, params.routeId);

        for (String stopId : params.allStopIds) {
            stmt.setString(i++, stopId);
        }
        
        if (params.directionId != null) {
            stmt.setInt(i++, params.directionId);
        }
        
        stmt.setString(i++, params.destinationId);

        if (params.journeyNote != null && params.journeyNote.length() == 4) {
            stmt.setString(i, params.journeyNote);
        }
    }

    /**
     * Builds a cache key for the trip finder query.
     */
    private static String buildCacheKey(
        String routeId,
        boolean isArrivalTime,
        String destinationId,
        String journeyNote,
        Integer directionId,
        List<EstimatedCall> estimatedCalls
    ) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(routeId).append('|')
                  .append(isArrivalTime ? 'A' : 'D').append('|')
                  .append(destinationId).append('|');
        if (journeyNote != null) keyBuilder.append(journeyNote).append('|');
        if (directionId != null) keyBuilder.append(directionId).append('|');
        for (EstimatedCall ec : estimatedCalls) {
            keyBuilder.append(ec.stopId()).append('@').append(ec.isoTime()).append('|');
        }
        return keyBuilder.toString();
    }

    /**
     * Finds the best matching theoretical time within the given time window.
     * Returns null if no time is within the window.
     */
    private static Integer findBestTheoreticalTime(
        List<Integer> theoreticalTimes,
        int realTime,
        int minWindow,
        int maxWindow
    ) {
        Integer bestTheo = null;
        int minDiff = Integer.MAX_VALUE;
        
        for (Integer theoTime : theoreticalTimes) {
            int normalizedTheoTime = theoTime;
            int diff = normalizedTheoTime - realTime;

            if (diff >= minWindow && diff <= maxWindow) {
                int absDiff = Math.abs(diff);
                if (absDiff < minDiff) {
                    minDiff = absDiff;
                    bestTheo = normalizedTheoTime;
                }
            }
        }
        
        return bestTheo;
    }

    /**
     * Evaluates a single trip against the estimated calls within the given time window.
     * Returns the total time difference if all stops match, or -1 if not all stops match.
     */
    private static long evaluateTripMatch(
        Map<String, List<Integer>> tripStops,
        List<EstimatedCall> estimatedCalls,
        ZoneId zone,
        int minWindow,
        int maxWindow
    ) {
        long totalDiff = 0;

        for (EstimatedCall ec : estimatedCalls) {
            String stopId = ec.stopId();
            int realTime = convertToSecondsSinceServiceDay(ec, zone);

            List<Integer> theoreticalTimes = tripStops.get(stopId);
            if (theoreticalTimes == null || theoreticalTimes.isEmpty()) {
                return -1; // Stop not matched
            }

            Integer bestTheo = findBestTheoreticalTime(theoreticalTimes, realTime, minWindow, maxWindow);
            if (bestTheo == null) {
                return -1; // No theoretical time within window
            }
            
            totalDiff += Math.abs(bestTheo - realTime);
        }

        return totalDiff;
    }

    /**
     * Finds the best matching trip from the candidate trips within the defined time windows.
     */
    private static String findBestMatchingTrip(
        Map<String, Map<String, List<Integer>>> tripStopTimes,
        List<EstimatedCall> estimatedCalls,
        ZoneId zone
    ) {
        int[][] windows = {
            {-1, 1},
            {-2, 2},
            {-3, 5},
            {-3, 10},
            {-3, 20},
            {-3, 30}
        };

        List<TripMatch> allMatches = new ArrayList<>();

        for (int[] window : windows) {
            int minWindow = window[0] * 60;
            int maxWindow = window[1] * 60;

            for (Map.Entry<String, Map<String, List<Integer>>> entry : tripStopTimes.entrySet()) {
                String tripId = entry.getKey();
                Map<String, List<Integer>> tripStops = entry.getValue();

                long totalDiff = evaluateTripMatch(tripStops, estimatedCalls, zone, minWindow, maxWindow);
                
                if (totalDiff >= 0) {
                    allMatches.add(new TripMatch(tripId, totalDiff));
                }
            }
        }

        allMatches.sort((a, b) -> Long.compare(a.totalDiff, b.totalDiff));

        return allMatches.isEmpty() ? null : allMatches.get(0).tripId;
    }

    /**
     * Finds the trip ID that best matches a sequence of real-time estimated calls.
     * 
     * <p>This method attempts to match real-time stop data (EstimatedCalls) to scheduled trips
     * by comparing theoretical stop times with real-time estimates. It uses a progressive time
     * window approach, starting with strict matching (-1/+1 minutes) and gradually expanding
     * to more lenient windows (up to -3/+30 minutes) until a match is found.
     * 
     * <p><b>Matching Algorithm:</b>
     * <ol>
     *   <li>Converts real-time timestamps to seconds since service day start</li>
     *   <li>Queries the database for candidate trips matching the route, service day, direction, and destination</li>
     *   <li>For each time window, calculates the total time difference for all stops</li>
     *   <li>Returns the trip with the smallest total time difference</li>
     * </ol>
     * 
     * <p><b>Time Windows (in minutes):</b> [-1,+1], [-2,+2], [-3,+5], [-3,+10], [-3,+20], [-3,+30]
     * 
     * <p>Results are cached to avoid repeated heavy database queries for identical inputs.
     * 
     * @param routeId The GTFS route ID to search within
     * @param estimatedCalls List of real-time stop estimates with stop IDs and timestamps
     * @param isArrivalTime If true, match on arrival times; if false, match on departure times
     * @param destinationId The final stop ID that the trip must reach
     * @param journeyNote Optional journey note to match against trip headsign or short name
     * @param journeyNoteDetailled If true, match journeyNote against trip_short_name; if false, against trip_headsign
     * @param directionId Optional direction filter (0 or 1), or null to match both directions
     * @return The matching trip ID, or null if no suitable match is found
     * @throws SQLException If a database error occurs
     * @throws IllegalArgumentException If routeId or estimatedCalls is null or empty
     */
    public static String findTripIdFromEstimatedCalls(
        String routeId,
        List<EstimatedCall> estimatedCalls,
        boolean isArrivalTime,
        String destinationId,
        String journeyNote,
        boolean journeyNoteDetailled,
        Integer directionId
    ) throws SQLException {
        if (routeId == null || estimatedCalls == null || estimatedCalls.isEmpty()) {
            throw new IllegalArgumentException("Inputs cannot be null or empty.");
        }

        // Check cache first
        String cacheKey = buildCacheKey(routeId, isArrivalTime, destinationId, journeyNote, directionId, estimatedCalls);
        if (findTripCache.containsKey(cacheKey)) {
            return findTripCache.get(cacheKey);
        }

        ZoneId zone = PARIS_ZONE;
        String timeColumn = isArrivalTime ? "arrival_timestamp" : "departure_timestamp";

        // Determine service day from estimated calls
        LocalDate serviceDay = determineServiceDayFromEstimatedCalls(estimatedCalls, zone);

        // Prepare query parameters
        String yyyymmdd = serviceDay.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        int dayOfWeek = serviceDay.getDayOfWeek().getValue();
        String weekday = String.valueOf(dayOfWeek == 7 ? 0 : dayOfWeek);
        List<String> allStopIds = estimatedCalls.stream()
            .map(EstimatedCall::stopId)
            .distinct()
            .toList();

        // Build query and fetch candidate trips from database
        String query = buildTripFinderQuery(timeColumn, allStopIds, directionId, journeyNote, journeyNoteDetailled);
        TripQueryParameters queryParams = new TripQueryParameters(
            yyyymmdd, weekday, routeId, allStopIds, directionId, destinationId, journeyNote
        );
        Map<String, Map<String, List<Integer>>> tripStopTimes = fetchCandidateTrips(query, queryParams);

        // Find best matching trip
        String result = findBestMatchingTrip(tripStopTimes, estimatedCalls, zone);
        
        // Cache and return result
        findTripCache.put(cacheKey, result);
        return result;
    }

    /**
     * Fetches candidate trips from the database based on the query and parameters.
     * 
     * @param query The SQL query to execute
     * @param params The query parameters encapsulated in a TripQueryParameters object
     * @return A map of trip IDs to their stop times
     * @throws SQLException If a database access error occurs
     */
    private static Map<String, Map<String, List<Integer>>> fetchCandidateTrips(
        String query,
        TripQueryParameters params
    ) throws SQLException {
        Map<String, Map<String, List<Integer>>> tripStopTimes = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            bindTripFinderParameters(stmt, params);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tripId = rs.getString("trip_id");
                    String stopId = rs.getString(COL_STOP_ID);
                    int stopTime = rs.getInt("stop_time");

                    tripStopTimes
                        .computeIfAbsent(tripId, k -> new HashMap<>())
                        .computeIfAbsent(stopId, k -> new ArrayList<>())
                        .add(stopTime);
                }
            }
        }

        return tripStopTimes;
    }

    /**
     * Utility class for sorting trip matches based on time difference.
     * Used internally by {@link #findTripIdFromEstimatedCalls} to rank potential matches.
     */
    private static class TripMatch {
        /** The trip identifier */
        String tripId;
        
        /** Total time difference in seconds between scheduled and real-time stops */
        long totalDiff;
        
        /**
         * Constructs a new TripMatch.
         * 
         * @param tripId The trip identifier
         * @param totalDiff Total time difference in seconds
         */
        TripMatch(String tripId, long totalDiff) {
            this.tripId = tripId;
            this.totalDiff = totalDiff;
        }
    }

    /**
     * Retrieves all stop times for a given trip in sequential order.
     * 
     * <p>Returns a list of strings where each string contains:
     * stop_id, arrival_timestamp, departure_timestamp, and stop_sequence (comma-separated).
     * 
     * <p>Results are cached to avoid repeated database queries for the same trip.
     * 
     * @param tripId The trip identifier to retrieve stop times for
     * @return List of comma-separated stop time data (format: "stopId,arrivalTime,departureTime,stopSequence")
     */
    public static List<String> getAllStopTimesFromTrip(String tripId) {
        // Try cache first
        List<String> cached = allStopTimesCache.get(tripId);
        if (cached != null) return cached;

        String query = "SELECT stop_id, arrival_timestamp, departure_timestamp, stop_sequence FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence;";
        List<String> results = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, tripId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String stopId = rs.getString(COL_STOP_ID);
                    String arrivalTime = rs.getString("arrival_timestamp");
                    String departureTime = rs.getString("departure_timestamp");
                    int stopSequence = rs.getInt("stop_sequence");

                    results.add(String.format("%s,%s,%s,%d", stopId, arrivalTime, departureTime, stopSequence));
                }
            }
            // populate cache
            allStopTimesCache.put(tripId, results);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return results;
    }

    /**
     * Finds the stop sequence number for a given stop within a trip.
     * 
     * <p>This method retrieves the stop sequence for a specific stop on a trip,
     * excluding any sequences that are already present in the stopUpdates list.
     * This is useful when building incremental trip updates to avoid duplicate entries.
     * 
     * <p>Results are cached to improve performance for repeated queries.
     * 
     * @param tripId The trip identifier
     * @param stopId The stop identifier
     * @param stopUpdates List of stop sequence numbers to exclude from results
     * @return The stop sequence number as a string, or null if not found
     */
    public static String findStopSequence(String tripId, String stopId, List<String> stopUpdates) {
        // Use cached mapping tripId -> (stopId -> list of sequences) if available
        Map<String, List<String>> seqMap = stopSequencesCache.get(tripId);
        if (seqMap == null) {
            // Build mapping from DB and cache it
            seqMap = new HashMap<>();
            String query = "SELECT stop_id, stop_sequence FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence;";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, tripId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String sId = rs.getString(COL_STOP_ID);
                        String seq = rs.getString("stop_sequence");
                        seqMap.computeIfAbsent(sId, k -> new ArrayList<>()).add(seq);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            stopSequencesCache.put(tripId, seqMap);
        }

        List<String> seqs = seqMap.get(stopId);
        if (seqs == null || seqs.isEmpty()) return null;

        // Return first sequence that's not in stopUpdates
        for (String s : seqs) {
            if (!stopUpdates.contains(s)) return s;
        }
        return null;
    }

    /**
     * Finds a child stop ID from the stop_extensions table for a given parent stop and trip.
     * 
     * <p>Some GTFS feeds have hierarchical stop structures where a parent stop (like a station)
     * has multiple child stops (like platforms). This method finds the specific child stop
     * that is used in a particular trip.
     * 
     * @param stopId The parent stop identifier (object_code in stop_extensions)
     * @param tripId The trip identifier to search within
     * @return The child stop ID (object_id), or null if not found
     */
    public static String findStopChildrenByTrip(String stopId, String tripId) {
        String childStopId = null;
        String query = """
            SELECT se.object_id
            FROM stop_extensions se
            JOIN stop_times st ON se.object_id = st.stop_id
            WHERE se.object_code = ? AND st.trip_id = ? LIMIT 1;
        """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, stopId);
            stmt.setString(2, tripId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    childStopId = rs.getString("object_id");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return childStopId;
    }

    /**
     * Checks if the stop_extensions table exists in the GTFS database.
     * 
     * <p>The stop_extensions table is a custom extension used by some GTFS feeds
     * to store additional stop metadata, particularly for hierarchical stop structures.
     * 
     * @return true if the stop_extensions table exists, false otherwise
     */
    public static boolean checkIfStopExtensionsTableExists() {
        String query = "SELECT name FROM sqlite_master WHERE type='table' AND name='stop_extensions';";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            return rs.next();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Finds a stop ID from the GTFS stops table using a stop code.
     * 
     * <p>This method searches for stops where the stop_id contains the given stop code
     * as a suffix (after a colon separator). This matches the common pattern where
     * stop IDs are formatted as "prefix:code".
     * 
     * @param stopCode The stop code to search for
     * @return The full stop ID, or null if not found
     */
    public static String findStopIdFromCode(String stopCode) {
        String query = "SELECT stop_id FROM stops WHERE stop_id LIKE ?;";
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, "%:" + stopCode);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(COL_STOP_ID);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Finds a stop ID from the stop_extensions table using a stop extension code.
     * 
     * <p>This method searches the stop_extensions table for entries matching the given
     * stop extension code in the 'netex_zder_quay' object system. This is specific to
     * certain GTFS feeds that use NeTEx extensions.
     * 
     * @param stopExtension The stop extension code to search for
     * @return The object ID (stop ID), or null if not found
     */
    public static String findStopIdFromStopExtension(String stopExtension) {
        String query = "SELECT object_id FROM stop_extensions WHERE object_code LIKE ? AND object_system LIKE 'netex_zder_quay' LIMIT 1;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, "%" + stopExtension);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("object_id");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /** Marker used in cache to distinguish between "not found" and "not cached" */
    private static final String EMPTY_MARKER = "__NULL__";
    
    /** 
     * Cache for stop code to stop ID mappings.
     * Maps stop codes to their resolved stop IDs to avoid repeated database queries.
     */
    public static final Map<String, String> stopCodeCache = new ConcurrentHashMap<>();

    /** Cache for trip metadata (tripId -> TripMeta) to reduce database access */
    private static final ConcurrentHashMap<String, TripMeta> tripMetaCache = new ConcurrentHashMap<>();
    
    /** Cache for stop times by trip (tripId -> list of stop time data strings) */
    private static final ConcurrentHashMap<String, List<String>> allStopTimesCache = new ConcurrentHashMap<>();
    
    /** 
     * Cache for stop sequences by trip and stop.
     * Maps tripId -> (stopId -> list of sequences as strings).
     */
    private static final ConcurrentHashMap<String, Map<String, List<String>>> stopSequencesCache = new ConcurrentHashMap<>();
    
    /** Maximum size of the LRU cache for trip finding results */
    private static final int FIND_TRIP_CACHE_SIZE = 5000;
    
    /** 
     * LRU cache for findTripIdFromEstimatedCalls results to avoid repeated heavy DB queries.
     * Uses an access-order LinkedHashMap with automatic eviction of eldest entries.
     */
    private static final java.util.Map<String, String> findTripCache = java.util.Collections.synchronizedMap(
        new java.util.LinkedHashMap<String, String>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(java.util.Map.Entry<String, String> eldest) {
                return size() > FIND_TRIP_CACHE_SIZE;
            }
        }
    );

    /**
     * Resolves a stop code to its full stop ID using multiple lookup strategies.
     * 
     * <p>This method first checks the cache, then attempts to find the stop ID:
     * <ol>
     *   <li>By searching the stops table for a matching stop code</li>
     *   <li>By searching the stop_extensions table if the first method fails</li>
     * </ol>
     * 
     * <p>Results are cached to improve performance for repeated lookups.
     * 
     * @param stopCode The stop code to resolve
     * @return The resolved stop ID, or null if not found
     * @see #findStopIdFromCode(String)
     * @see #findStopIdFromStopExtension(String)
     */
    public static String resolveStopId(String stopCode) {
        String cached = stopCodeCache.get(stopCode);
        if (cached != null) {
            return EMPTY_MARKER.equals(cached) ? null : cached;
        }

        String stopId = TripFinder.findStopIdFromCode(stopCode);
        if (stopId == null) {
            stopId = TripFinder.findStopIdFromStopExtension(stopCode);
        }

        stopCodeCache.put(stopCode, stopId == null ? EMPTY_MARKER : stopId);
        return stopId;
    }

    /**
     * Retrieves the scheduled arrival time for a specific stop in a trip.
     * 
     * <p>Searches through the provided stop times list for a matching stop ID and sequence,
     * then converts the scheduled arrival time to epoch seconds.
     * 
     * <p><b>Note:</b> The arrival time may be >= 86400 seconds for trips that continue
     * past midnight on the same service day (e.g., 24:30:00 = 88200 seconds).
     * 
     * @param stopTimes List of stop time data (format: "stopId,arrivalTime,departureTime,stopSequence")
     * @param stopId The stop identifier to find
     * @param stopSequence The stop sequence number to match
     * @param zoneId The timezone for time calculations
     * @return The scheduled arrival time as epoch seconds, or null if not found
     */
    public static Long getScheduledArrivalTime(List<String> stopTimes, String stopId, String stopSequence, ZoneId zoneId) {
        for (String stopTime : stopTimes) {
            String[] parts = stopTime.split(",");
            if (parts[0].equals(stopId) && parts[3].equals(stopSequence)) {
                String arrivalTimeCollected = parts[1];
                if (arrivalTimeCollected != null && !arrivalTimeCollected.isEmpty()) {
            long serviceDayEpoch = Instant.now().atZone(zoneId)
                .toLocalDateTime()
                .toLocalDate()
                .atStartOfDay(zoneId)
                .toEpochSecond();
            // arrivalTimeCollected is seconds since service day's start and may be >= 86400
            return serviceDayEpoch + Long.parseLong(arrivalTimeCollected);
                }
            }
        }
        return null;
    }

    /**
     * Retrieves the scheduled departure time for a specific stop in a trip.
     * 
     * <p>Searches through the provided stop times list for a matching stop ID and sequence,
     * then converts the scheduled departure time to epoch seconds.
     * 
     * <p><b>Note:</b> The departure time may be >= 86400 seconds for trips that continue
     * past midnight on the same service day (e.g., 25:15:00 = 90900 seconds).
     * 
     * @param stopTimes List of stop time data (format: "stopId,arrivalTime,departureTime,stopSequence")
     * @param stopId The stop identifier to find
     * @param stopSequence The stop sequence number to match
     * @param zoneId The timezone for time calculations
     * @return The scheduled departure time as epoch seconds, or null if not found
     */
    public static Long getScheduledDepartureTime(List<String> stopTimes, String stopId, String stopSequence, ZoneId zoneId) {
        for (String stopTime : stopTimes) {
            String[] parts = stopTime.split(",");
            if (parts[0].equals(stopId) && parts[3].equals(stopSequence)) {
                String departureTimeCollected = parts[2];
                if (departureTimeCollected != null && !departureTimeCollected.isEmpty()) {
            long serviceDayEpoch = Instant.now().atZone(zoneId)
                .toLocalDateTime()
                .toLocalDate()
                .atStartOfDay(zoneId)
                .toEpochSecond();
            // departureTimeCollected is seconds since service day's start and may be >= 86400
            return serviceDayEpoch + Long.parseLong(departureTimeCollected);
                }
            }
        }
        return null;
    }

    /**
     * Retrieves the direction ID for a given trip.
     * 
     * <p>First checks the cache for trip metadata, then queries the database if needed.
     * Direction ID is typically 0 for outbound and 1 for inbound, but conventions vary by agency.
     * 
     * @param tripId The trip identifier
     * @return The direction ID as a string, or null if not found
     */
    public static String getTripDirection(String tripId) {
        TripMeta cached = tripMetaCache.get(tripId);
        if (cached != null) return String.valueOf(cached.directionId);

        String query = "SELECT direction_id FROM trips WHERE trip_id = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, tripId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String dir = rs.getString(COL_DIRECTION_ID);
                    // no full TripMeta constructed here because cost is small; but we can populate tripMetaCache via getTripMeta
                    TripMeta meta = getTripMeta(tripId);
                    if (meta != null) tripMetaCache.put(tripId, meta);
                    return dir;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Returns all trips with minimal metadata that are active today for the given route IDs.
     * 
     * <p>This method uses the GTFS calendar and calendar_dates tables to determine which
     * trips are running on the current service day. It includes:
     * <ul>
     *   <li>Trips from regular service patterns (calendar table)</li>
     *   <li>Trips added for today (calendar_dates with exception_type = 1)</li>
     *   <li>Excludes trips removed for today (calendar_dates with exception_type = 2)</li>
     * </ul>
     * 
     * <p>Each trip includes the first stop time (earliest arrival or departure) which can
     * be used for filtering trips by time window.
     * 
     * @param routeIds List of route identifiers to query
     * @return List of TripMeta objects containing trip ID, route ID, direction, and first stop time
     */
    public static List<TripMeta> getActiveTripsForRoutesToday(List<String> routeIds) {
        if (routeIds == null || routeIds.isEmpty()) return java.util.Collections.emptyList();

        ZoneId zone = PARIS_ZONE;
        LocalDate date = LocalDate.now(zone);
        String yyyymmdd = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        int dayOfWeek = date.getDayOfWeek().getValue();
        String weekday = String.valueOf(dayOfWeek == 7 ? 0 : dayOfWeek);

        String inClause = routeIds.stream().map(r -> "?").collect(Collectors.joining(","));

        String sql = (
            "WITH valid_services AS (\n" +
            "    SELECT service_id FROM calendar\n" +
            "    WHERE start_date <= ? AND end_date >= ?\n" +
            "    AND (\n" +
            "        (monday = 1 AND ? = '1') OR\n" +
            "        (tuesday = 1 AND ? = '2') OR\n" +
            "        (wednesday = 1 AND ? = '3') OR\n" +
            "        (thursday = 1 AND ? = '4') OR\n" +
            "        (friday = 1 AND ? = '5') OR\n" +
            "        (saturday = 1 AND ? = '6') OR\n" +
            "        (sunday = 1 AND ? = '0')\n" +
            "    )\n" +
            "    UNION\n" +
            "    SELECT service_id FROM calendar_dates\n" +
            "    WHERE date = ? AND exception_type = 1\n" +
            "),\n" +
            "excluded_services AS (\n" +
            "    SELECT service_id FROM calendar_dates\n" +
            "    WHERE date = ? AND exception_type = 2\n" +
            ")\n" +
            "SELECT t.trip_id, t.route_id, t.direction_id, MIN(COALESCE(st.departure_timestamp, st.arrival_timestamp)) AS first_time\n" +
            "FROM trips t\n" +
            "JOIN stop_times st ON st.trip_id = t.trip_id\n" +
            "WHERE t.route_id IN (" + inClause + ")\n" +
            "AND t.service_id IN (SELECT service_id FROM valid_services)\n" +
            "AND t.service_id NOT IN (SELECT service_id FROM excluded_services)\n" +
            "GROUP BY t.trip_id, t.route_id, t.direction_id\n"
        );

        List<TripMeta> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int i = 1;
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, yyyymmdd);
            for (int j = 0; j < 7; j++) stmt.setString(i++, weekday);
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, yyyymmdd);
            for (String r : routeIds) stmt.setString(i++, r);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tripId = rs.getString("trip_id");
                    String routeId = rs.getString("route_id");
                    int dir = rs.getInt(COL_DIRECTION_ID);
                    int first = rs.getInt("first_time");
                    // first may already be seconds since service day; keep raw value but cast into 0..int range
                    result.add(new TripMeta(tripId, routeId, dir, first));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Returns trip metadata for a given trip ID.
     * 
     * <p>Retrieves essential trip information including route ID, direction ID, and the
     * first stop time of the trip. This metadata is useful for building GTFS Realtime
     * trip descriptors and for trip matching operations.
     * 
     * <p>Results are cached to improve performance for repeated queries.
     * 
     * @param tripId The trip identifier to retrieve metadata for
     * @return TripMeta object containing trip metadata, or null if the trip is not found
     * @see TripMeta
     */
    public static TripMeta getTripMeta(String tripId) {
        TripMeta cached = tripMetaCache.get(tripId);
        if (cached != null) return cached;

        String sql = "SELECT t.trip_id, t.route_id, t.direction_id, MIN(COALESCE(st.departure_timestamp, st.arrival_timestamp)) AS first_time " +
                "FROM trips t JOIN stop_times st ON st.trip_id = t.trip_id WHERE t.trip_id = ? GROUP BY t.trip_id, t.route_id, t.direction_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tripId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String routeId = rs.getString("route_id");
                    int dir = rs.getInt(COL_DIRECTION_ID);
                    int first = rs.getInt("first_time");
                    TripMeta meta = new TripMeta(tripId, routeId, dir, first);
                    tripMetaCache.put(tripId, meta);
                    return meta;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}