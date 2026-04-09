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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /** Logger instance for debugging and error reporting */
    private static final Logger logger = LoggerFactory.getLogger(TripFinder.class);
    
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
         * Last stop time in seconds since start of service day.
         * Used to check if a trip is still active at a given time.
         */
        public final int lastTimeSecOfDay;

        /**
         * The service date for this trip in YYYYMMDD format (e.g., "20231122").
         * This is used to populate the start_date field in GTFS-RT TripDescriptor.
         */
        public final String startDate;

        /**
         * Constructs a new TripMeta instance.
         *
         * @param tripId The unique trip identifier
         * @param routeId The route identifier
         * @param directionId The direction of travel (0 or 1)
         * @param firstTimeSecOfDay First stop time in seconds since service day start
         * @param lastTimeSecOfDay Last stop time in seconds since service day start
         * @param startDate The service date in YYYYMMDD format
         */
        public TripMeta(String tripId, String routeId, int directionId, int firstTimeSecOfDay, int lastTimeSecOfDay, String startDate) {
            this.tripId = tripId;
            this.routeId = routeId;
            this.directionId = directionId;
            this.firstTimeSecOfDay = firstTimeSecOfDay;
            this.lastTimeSecOfDay = lastTimeSecOfDay;
            this.startDate = startDate;
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
     * Determines the service day for a given timestamp.
     * Simply returns the calendar date of the timestamp.
     * The midnight-crossing logic is now handled dynamically in convertToSecondsSinceServiceDay.
     * 
     * @param timestamp The real-time timestamp to determine service day for
     * @param zone The timezone to use
     * @return The service day (calendar day of the timestamp)
     */
    private static LocalDate determineServiceDay(ZonedDateTime timestamp, ZoneId zone) {
        return timestamp.toLocalDate();
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
     * Handles midnight-crossing trips by returning values >= 86400 for times after midnight.
     */
    private static int convertToSecondsSinceServiceDay(
        EstimatedCall ec,
        ZoneId zone
    ) {
        Instant inst = Instant.parse(ec.isoTime());
        ZonedDateTime zdtEc = inst.atZone(zone);
        LocalDate calendarDay = zdtEc.toLocalDate();
        int secOfDay = (int) (zdtEc.toEpochSecond() - calendarDay.atStartOfDay(zone).toEpochSecond());
        
        // If it's early morning (after midnight), return the value as-is for comparison with GTFS times
        // This will be compared against both regular times (< 86400) and midnight-crossing times (>= 86400)
        return secOfDay;
    }

    /**
     * Builds the SQL query for finding candidate trips.
     */
    private static String buildTripFinderQuery(
        String timeColumn,
        List<String> allStopIds,
        Integer directionId,
        String journeyNote,
        boolean journeyNoteDetailled,
        boolean partialDestination
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

        if (partialDestination) {
            query.append("""
                AND (
                    st.trip_id NOT NULL
                    AND EXISTS (
                        SELECT 1 FROM stop_times st2
                        WHERE st2.trip_id = st.trip_id
                        AND st2.stop_id = ?
                    )
                )
            """);
        } else {
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
        }

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
     * Handles midnight-crossing trips by trying both the actual time and the time + 24h.
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
            // Try matching with the real time as-is
            int diff = theoTime - realTime;
            if (diff >= minWindow && diff <= maxWindow) {
                int absDiff = Math.abs(diff);
                if (absDiff < minDiff) {
                    minDiff = absDiff;
                    bestTheo = theoTime;
                }
            }
            
            // If the theoretical time is >= 86400 (midnight-crossing), also try matching
            // with realTime + 86400 (in case the real-time data is for yesterday's service day)
            if (theoTime >= 86400) {
                int realTimeAdjusted = realTime + 86400;
                diff = theoTime - realTimeAdjusted;
                if (diff >= minWindow && diff <= maxWindow) {
                    int absDiff = Math.abs(diff);
                    if (absDiff < minDiff) {
                        minDiff = absDiff;
                        bestTheo = theoTime;
                    }
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
        int matchedStops = 0;
        // Require at least half the stops (min 2) to be present in the candidate trip.
        // This tolerates SIRI stops that are absent from the GTFS (e.g. new stops with
        // non-standard codes like 6-digit SP:474069) without rejecting the whole trip.
        int requiredStops = Math.max(2, (estimatedCalls.size() + 1) / 2);

        for (EstimatedCall ec : estimatedCalls) {
            String stopId = ec.stopId();
            int realTime = convertToSecondsSinceServiceDay(ec, zone);

            List<Integer> theoreticalTimes = tripStops.get(stopId);
            if (theoreticalTimes == null || theoreticalTimes.isEmpty()) {
                // Stop not served by this candidate trip — skip rather than reject.
                // The stop may be a SIRI-only code not yet in GTFS.
                continue;
            }

            Integer bestTheo = findBestTheoreticalTime(theoreticalTimes, realTime, minWindow, maxWindow);
            if (bestTheo == null) {
                // Stop IS in this trip but the time is outside the window → wrong trip.
                return -1;
            }

            totalDiff += Math.abs(bestTheo - realTime);
            matchedStops++;
        }

        if (matchedStops < requiredStops) {
            return -1; // Too few stops matched — candidate trip is not a credible match.
        }

        return totalDiff;
    }

    /**
     * Finds the best matching trip from the candidate trips within the defined time windows.
     * Time windows are progressively expanded to handle various delay scenarios in Île-de-France.
     * Windows are adapted based on the route type (metro/tram are more punctual than buses).
     * 
     * @param tripStopTimes Map of candidate trips with their stop times
     * @param estimatedCalls List of real-time estimated calls
     * @param zone Time zone for conversions
     * @param routeType GTFS route type (0=Tram, 1=Metro, 2=Rail, 3=Bus, etc.)
     * @return The best matching trip ID, or null if no match found
     */
    private static String findBestMatchingTrip(
        Map<String, Map<String, List<Integer>>> tripStopTimes,
        List<EstimatedCall> estimatedCalls,
        ZoneId zone,
        int routeType
    ) {
        // Adapt time windows based on route type
        // Metro (1) and Tram (0): stricter windows (more punctual)
        // Bus (3): more lenient windows (more subject to traffic delays)
        // Rail (2): moderate windows
        int[][] windows;
        
        if (routeType == 0 || routeType == 1) {
            // Metro/Tram: strict matching
            windows = new int[][]{
                {-1, 1},   // ±1 minute: strict matching
                {-2, 3},   // -2 to +3 minutes: small delays
                {-3, 5},   // -3 to +5 minutes: moderate delays
                {-5, 10},  // -5 to +10 minutes: significant delays
                {-5, 15}   // -5 to +15 minutes: major delays (max for metro/tram)
            };
        } else if (routeType == 3) {
            // Bus: more lenient matching
            windows = new int[][]{
                {-2, 2},   // ±2 minutes: strict matching
                {-3, 5},   // -3 to +5 minutes: normal delays
                {-5, 10},  // -5 to +10 minutes: moderate delays
                {-5, 15},  // -5 to +15 minutes: significant delays
                {-5, 20},  // -5 to +20 minutes: major delays
                {-5, 30}   // -5 to +30 minutes: severe delays (max for buses)
            };
        } else {
            // Rail/Other: moderate matching
            windows = new int[][]{
                {-2, 2},   // ±2 minutes: strict matching
                {-3, 5},   // -3 to +5 minutes: normal delays
                {-5, 10},  // -5 to +10 minutes: moderate delays
                {-5, 15},  // -5 to +15 minutes: significant delays
                {-5, 20}   // -5 to +20 minutes: major delays
            };
        }

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
     * Attempts to find a GTFS trip ID by directly matching the SIRI DatedVehicleJourneyRef
     * against trip_id values in the database.
     *
     * <p>For operators like SNCF/Transilien, the SIRI {@code DatedVehicleJourneyRef} value is
     * typically a UUID suffix that appears verbatim in the GTFS {@code trip_id}
     * (e.g. {@code IDFM:TN:SNCF:<uuid>}). This method looks up the trip using a
     * {@code LIKE '%<vehicleRef>'} pattern and validates that the result belongs to
     * the expected route, avoiding false positives from other operators.
     *
     * @param vehicleRef the raw DatedVehicleJourneyRef value from SIRI
     * @param routeId    the expected GTFS route ID (used to validate the match)
     * @return the matching trip_id, or {@code null} if not found or on DB error
     */
    public static String findTripIdByVehicleRef(String vehicleRef, String routeId) {
        if (vehicleRef == null || vehicleRef.isEmpty() || routeId == null) {
            return null;
        }
        String query = "SELECT trip_id FROM trips WHERE trip_id LIKE ? AND route_id = ? LIMIT 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, "%" + vehicleRef);
            stmt.setString(2, routeId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("trip_id");
                }
            }
        } catch (SQLException e) {
            logger.debug("findTripIdByVehicleRef error for vehicleRef={}: {}", vehicleRef, e.getMessage());
        }
        return null;
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
        
        // For early morning times (between 00:00 and 06:00), also check trips from yesterday's service day
        // because they might have GTFS times >= 24:00:00 (midnight-crossing trips)
        // Example: A bus with GTFS time 28:00:00 (04:00 AM) belongs to yesterday's service day
        EstimatedCall firstCall = estimatedCalls.get(0);
        Instant firstInstant = Instant.parse(firstCall.isoTime());
        ZonedDateTime firstZdt = firstInstant.atZone(zone);
        boolean isEarlyMorning = firstZdt.getHour() < 8;
        
        // Try current service day first (destination must be the terminus)
        String result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                                   journeyNote, journeyNoteDetailled, directionId,
                                                   serviceDay, zone, timeColumn, false);

        // If no match and it's early morning, also try yesterday's service day
        if (result == null && isEarlyMorning) {
            LocalDate previousServiceDay = serviceDay.minusDays(1);
            result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                              journeyNote, journeyNoteDetailled, directionId,
                                              previousServiceDay, zone, timeColumn, false);
        }

        // If still no match and a directionId was specified, retry without direction constraint.
        // Some GTFS producers (e.g. RCASO for T12) use direction_id conventions that differ
        // from the inbound/outbound mapping derived from SIRI DirectionRef.
        if (result == null && directionId != null) {
            result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                              journeyNote, journeyNoteDetailled, null,
                                              serviceDay, zone, timeColumn, false);
            if (result == null && isEarlyMorning) {
                LocalDate previousServiceDay = serviceDay.minusDays(1);
                result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                                  journeyNote, journeyNoteDetailled, null,
                                                  previousServiceDay, zone, timeColumn, false);
            }
        }

        // Fallback for partial trips: destination is an intermediate stop, not the terminus.
        if (result == null && destinationId != null) {
            result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                              journeyNote, journeyNoteDetailled, directionId,
                                              serviceDay, zone, timeColumn, true);
            if (result == null && isEarlyMorning) {
                LocalDate previousServiceDay = serviceDay.minusDays(1);
                result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                                  journeyNote, journeyNoteDetailled, directionId,
                                                  previousServiceDay, zone, timeColumn, true);
            }
            if (result == null && directionId != null) {
                result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                                  journeyNote, journeyNoteDetailled, null,
                                                  serviceDay, zone, timeColumn, true);
                if (result == null && isEarlyMorning) {
                    LocalDate previousServiceDay = serviceDay.minusDays(1);
                    result = searchTripsForServiceDay(routeId, estimatedCalls, isArrivalTime, destinationId,
                                                      journeyNote, journeyNoteDetailled, null,
                                                      previousServiceDay, zone, timeColumn, true);
                }
            }
        }
        
        // Cache and return result
        findTripCache.put(cacheKey, result);
        return result;
    }

    /**
     * Searches for matching trips for a specific service day.
     *
     * @param partialDestination when false, destinationId must be the last stop;
     *                           when true, it may appear anywhere (partial/short-turn trips)
     */
    private static String searchTripsForServiceDay(
        String routeId,
        List<EstimatedCall> estimatedCalls,
        boolean isArrivalTime,
        String destinationId,
        String journeyNote,
        boolean journeyNoteDetailled,
        Integer directionId,
        LocalDate serviceDay,
        ZoneId zone,
        String timeColumn,
        boolean partialDestination
    ) throws SQLException {
        // Prepare query parameters
        String yyyymmdd = serviceDay.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        int dayOfWeek = serviceDay.getDayOfWeek().getValue();
        String weekday = String.valueOf(dayOfWeek == 7 ? 0 : dayOfWeek);
        List<String> allStopIds = estimatedCalls.stream()
            .map(EstimatedCall::stopId)
            .distinct()
            .toList();

        // Build query and fetch candidate trips from database
        String query = buildTripFinderQuery(timeColumn, allStopIds, directionId, journeyNote, journeyNoteDetailled, partialDestination);
        TripQueryParameters queryParams = new TripQueryParameters(
            yyyymmdd, weekday, routeId, allStopIds, directionId, destinationId, journeyNote
        );
        Map<String, Map<String, List<Integer>>> tripStopTimes = fetchCandidateTrips(query, queryParams);

        if (tripStopTimes.isEmpty()) {
            return null;
        }

        // Get route type to adapt matching windows
        int routeType = getRouteType(routeId);

        // Find best matching trip
        return findBestMatchingTrip(tripStopTimes, estimatedCalls, zone, routeType);
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
     * Retrieves the route type for a given route ID from the GTFS routes table.
     * Route types follow the GTFS specification:
     * 0 = Tram, 1 = Metro, 2 = Rail, 3 = Bus, 4 = Ferry, etc.
     * 
     * @param routeId The GTFS route ID
     * @return The route type, or 3 (Bus) as default if not found
     */
    public static int getRouteType(String routeId) {
        if (routeId == null || routeId.isEmpty()) {
            return 3; // Default to Bus
        }
        
        String query = "SELECT route_type FROM routes WHERE route_id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setString(1, routeId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("route_type");
                }
            }
        } catch (SQLException e) {
            logger.debug("Error getting route type for route: {}", routeId, e);
        }
        
        return 3; // Default to Bus if not found
    }

    /**
     * Finds the best matching GTFS trip for a single stop and expected departure/arrival time.
     *
     * <p>Used for operators whose EstimatedVehicleJourney entries contain EstimatedCalls that
     * each represent a distinct vehicle at that stop rather than sequential stops of one vehicle.
     *
     * <p>The method queries active trips for the given route and service day, filtering to those
     * that serve the given stop within the configured time window, and returns the one whose
     * scheduled stop time is closest to the real-time value.
     *
     * @param routeId       The GTFS route ID
     * @param stopId        The stop ID where the real-time event is observed
     * @param isoTime       The ISO 8601 real-time departure/arrival timestamp
     * @param directionId   Optional direction filter (0 or 1), or null for either direction
     * @param destinationId Optional last-stop ID the matched trip must end at, or null
     * @return The matching trip ID, or null if no suitable match is found
     * @throws SQLException If a database error occurs
     */
    public static String findTripForSingleStop(
        String routeId,
        String stopId,
        String isoTime,
        Integer directionId,
        String destinationId
    ) throws SQLException {
        if (routeId == null || stopId == null || isoTime == null) {
            return null;
        }

        String cacheKey = routeId + "|" + stopId + "|" + isoTime + "|"
                + (directionId != null ? directionId : "") + "|"
                + (destinationId != null ? destinationId : "");
        String cached = findTripForSingleStopCache.get(cacheKey);
        if (cached != null) {
            return EMPTY_MARKER.equals(cached) ? null : cached;
        }

        ZoneId zone = PARIS_ZONE;
        Instant instant = Instant.parse(isoTime);
        ZonedDateTime zdt = instant.atZone(zone);
        LocalDate serviceDay = zdt.toLocalDate();
        int secOfDay = (int) (zdt.toEpochSecond() - serviceDay.atStartOfDay(zone).toEpochSecond());
        boolean isEarlyMorning = zdt.getHour() < 8;

        String result = findTripForSingleStopOnDay(routeId, stopId, secOfDay, directionId, destinationId, false, serviceDay);

        if (result == null && isEarlyMorning) {
            // For midnight-crossing trips the GTFS time is >= 86400; adjust accordingly
            int secOfDayExtended = secOfDay + 86400;
            result = findTripForSingleStopOnDay(routeId, stopId, secOfDayExtended, directionId, destinationId, false, serviceDay.minusDays(1));
        }

        // Fallback for partial trips: the destination is an intermediate stop, not the terminus.
        // Relax the constraint so that destinationId can appear anywhere in the trip.
        if (result == null && destinationId != null) {
            result = findTripForSingleStopOnDay(routeId, stopId, secOfDay, directionId, destinationId, true, serviceDay);
            if (result == null && isEarlyMorning) {
                int secOfDayExtended = secOfDay + 86400;
                result = findTripForSingleStopOnDay(routeId, stopId, secOfDayExtended, directionId, destinationId, true, serviceDay.minusDays(1));
            }
        }

        findTripForSingleStopCache.put(cacheKey, result != null ? result : EMPTY_MARKER);
        return result;
    }

    /**
     * Executes the single-stop trip search for a specific service day.
     *
     * @param partialDestination when false, destinationId must be the last stop of the trip;
     *                           when true, it may appear anywhere (for partial/short-turn trips)
     */
    private static String findTripForSingleStopOnDay(
        String routeId,
        String stopId,
        int targetSecOfDay,
        Integer directionId,
        String destinationId,
        boolean partialDestination,
        LocalDate serviceDay
    ) throws SQLException {
        String yyyymmdd = serviceDay.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        int dayOfWeek = serviceDay.getDayOfWeek().getValue();
        String weekday = String.valueOf(dayOfWeek == 7 ? 0 : dayOfWeek);
        int routeType = getRouteType(routeId);
        // 15 min window for metro/tram, 30 min for bus/other
        int windowSeconds = (routeType == 0 || routeType == 1) ? 900 : 1800;

        StringBuilder queryBuilder = new StringBuilder("""
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
            SELECT t.trip_id
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            WHERE t.route_id = ?
            AND st.stop_id = ?
            AND t.service_id IN (SELECT service_id FROM valid_services)
            AND t.service_id NOT IN (SELECT service_id FROM excluded_services)
            AND ABS(COALESCE(st.departure_timestamp, st.arrival_timestamp) - ?) <= ?
        """);

        if (directionId != null) {
            queryBuilder.append("AND t.direction_id = ?\n");
        }
        if (destinationId != null) {
            if (partialDestination) {
                queryBuilder.append("""
                    AND EXISTS (
                        SELECT 1 FROM stop_times st2
                        WHERE st2.trip_id = st.trip_id
                        AND st2.stop_id = ?
                    )
                """);
            } else {
                queryBuilder.append("""
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
                """);
            }
        }
        queryBuilder.append("ORDER BY ABS(COALESCE(st.departure_timestamp, st.arrival_timestamp) - ?) ASC\nLIMIT 1");

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString())) {
            int i = 1;
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, yyyymmdd);
            for (int j = 0; j < 7; j++) stmt.setString(i++, weekday);
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, routeId);
            stmt.setString(i++, stopId);
            stmt.setInt(i++, targetSecOfDay);
            stmt.setInt(i++, windowSeconds);
            if (directionId != null) stmt.setInt(i++, directionId);
            if (destinationId != null) stmt.setString(i++, destinationId);
            stmt.setInt(i, targetSecOfDay);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("trip_id");
                }
            }
        }
        return null;
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
        if (tripId == null || tripId.isEmpty()) {
            return new ArrayList<>();
        }
        
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
            logger.debug("Error getting all stop times for trip: {}", tripId, e);
        }

        return results;
    }

    /**
     * Builds a reverse mapping from SIRI stop codes to GTFS stop IDs for all stops in a trip.
     *
     * <p>Unlike {@link #resolveStopId}, which looks up an arbitrary stop code globally in the
     * stops table (and may return a wrong sibling quai), this method enumerates all
     * {@code object_codes_extension} codes associated with each stop that actually belongs to the
     * given trip. The result is therefore scoped to the trip's stop set, which avoids the
     * false-resolution problem where two stops at the same physical location share similar codes.
     *
     * @param tripId the GTFS trip ID whose stops are to be indexed
     * @return map from SIRI stop code → GTFS stop_id (for stops on this trip only)
     */
    public static Map<String, String> buildSiriCodeToStopIdForTrip(String tripId) {
        List<String> stopRows = getAllStopTimesFromTrip(tripId);
        if (stopRows.isEmpty()) return java.util.Collections.emptyMap();

        List<String> stopIds = new ArrayList<>();
        Map<String, String> result = new java.util.LinkedHashMap<>();
        for (String row : stopRows) {
            String stopId = row.split(",", 2)[0];
            stopIds.add(stopId);
            // Always include the bare numeric suffix as a fallback code
            String bare = stopId.contains(":") ? stopId.substring(stopId.lastIndexOf(':') + 1) : stopId;
            result.put(bare, stopId);
        }

        if (stopIds.isEmpty()) return result;

        // Single batch query: fetch all object codes for the trip's stop IDs at once
        String placeholders = String.join(",", java.util.Collections.nCopies(stopIds.size(), "?"));
        String query = "SELECT object_id, object_code FROM object_codes_extension"
                + " WHERE object_id IN (" + placeholders + ") AND object_type = 'stop_point'";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < stopIds.size(); i++) {
                stmt.setString(i + 1, stopIds.get(i));
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString("object_code"), rs.getString("object_id"));
                }
            }
        } catch (SQLException e) {
            logger.debug("Error building SIRI code map for trip {}: {}", tripId, e.getMessage());
        }
        return result;
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
        if (tripId == null || tripId.isEmpty() || stopId == null || stopId.isEmpty()) {
            return null;
        }
        
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
                logger.debug("Error getting stop sequences for trip: {}", tripId, e);
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
     * Finds a child stop ID from the object_codes_extension table for a given parent stop and trip.
     * 
     * <p>Some GTFS feeds have hierarchical stop structures where a parent stop (like a station)
     * has multiple child stops (like platforms). This method finds the specific child stop
     * that is used in a particular trip.
     * 
     * @param stopId The parent stop identifier (object_code in object_codes_extension)
     * @param tripId The trip identifier to search within
     * @return The child stop ID (object_id), or null if not found
     */
    public static String findStopChildrenByTrip(String stopId, String tripId) {
        if (stopId == null || tripId == null) {
            return null;
        }
        
        String childStopId = null;
        String query = """
            SELECT se.object_id
            FROM object_codes_extension se
            JOIN stop_times st ON se.object_id = st.stop_id
            WHERE se.object_code = ? AND st.trip_id = ?
            AND se.object_type IN ('stop_area', 'stop_point') LIMIT 1;
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
            logger.debug("Error finding child stop ID for stopId: {} and tripId: {}", stopId, tripId, e);
        }

        return childStopId;
    }

    /**
     * Checks if the object_codes_extension table exists in the GTFS database.
     * 
     * <p>The object_codes_extension table is a custom extension used by IDFM GTFS feeds
     * to store additional object metadata (stops, routes, trips, agencies, etc.).
     * It replaces the former stop_extensions table.
     * 
     * @return true if the object_codes_extension table exists, false otherwise
     */
    public static boolean checkIfObjectCodesExtensionTableExists() {
        String query = "SELECT name FROM sqlite_master WHERE type='table' AND name='object_codes_extension';";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            return rs.next();
        } catch (Exception e) {
            logger.debug("Error checking if object_codes_extension table exists", e);
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
        if (stopCode == null) {
            return null;
        }
        
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
            logger.debug("Error finding stop ID from code: {}", stopCode, e);
        }
        return null;
    }

    /**
     * Finds a stop ID from the object_codes_extension table using a stop extension code.
     * 
     * <p>This method searches the object_codes_extension table for stop_point entries
     * matching the given stop extension code in the 'netex_zder_quay' object system.
     * This is specific to certain GTFS feeds that use NeTEx extensions.
     * 
     * @param stopExtension The stop extension code to search for
     * @return The object ID (stop ID), or null if not found
     */
    public static String findStopIdFromStopExtension(String stopExtension) {
        if (stopExtension == null) {
            return null;
        }
        
        String query = "SELECT object_id FROM object_codes_extension WHERE object_code LIKE ? AND object_system LIKE 'netex_zder_quay' AND object_type = 'stop_point' LIMIT 1;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, "%" + stopExtension);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("object_id");
                }
            }
        } catch (SQLException e) {
            logger.debug("Error finding stop ID from extension: {}", stopExtension, e);
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
     * LRU cache for findTripForSingleStop results to avoid repeated DB queries.
     */
    private static final java.util.Map<String, String> findTripForSingleStopCache = java.util.Collections.synchronizedMap(
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
     *   <li>By searching the object_codes_extension table if the first method fails</li>
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
        if (stopCode == null || stopCode.isEmpty()) {
            return null;
        }
        
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
        if (stopTimes == null || stopId == null || stopSequence == null) {
            return null;
        }
        
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
        if (stopTimes == null || stopId == null || stopSequence == null) {
            return null;
        }
        
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
        if (tripId == null || tripId.isEmpty()) {
            return null;
        }
        
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
            logger.debug("Error finding direction ID for trip: {}", tripId, e);
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
        LocalDate today = LocalDate.now(zone);
        List<TripMeta> result = new ArrayList<>(getActiveTripsForRoutesOnDate(routeIds, today));

        // Before 8 am, overnight trips whose service date is yesterday (firstTimeSecOfDay >= 86400)
        // are still running — include them too.
        if (java.time.LocalTime.now(zone).getHour() < 8) {
            result.addAll(getActiveTripsForRoutesOnDate(routeIds, today.minusDays(1)));
        }
        return result;
    }

    /**
     * Returns all active trips for the given routes on a specific service date.
     *
     * @param routeIds  GTFS route IDs to query
     * @param date      service date to use for calendar lookup
     * @return list of TripMeta, one entry per trip
     */
    public static List<TripMeta> getActiveTripsForRoutesOnDate(List<String> routeIds, LocalDate date) {
        if (routeIds == null || routeIds.isEmpty()) return java.util.Collections.emptyList();

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
            "SELECT t.trip_id, t.route_id, t.direction_id, MIN(COALESCE(st.departure_timestamp, st.arrival_timestamp)) AS first_time, MAX(COALESCE(st.arrival_timestamp, st.departure_timestamp)) AS last_time\n" +
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
                    int last = rs.getInt("last_time");
                    result.add(new TripMeta(tripId, routeId, dir, first, last, yyyymmdd));
                }
            }
        } catch (SQLException e) {
            logger.debug("Error getting active trips for route IDs on date {}", date, e);
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
    /**
     * Returns trip metadata for a given trip ID with service date.
     * 
     * @param tripId The trip identifier
     * @param serviceDate The service date in YYYYMMDD format (uses current date if null)
     * @return TripMeta object or null if not found
     */
    public static TripMeta getTripMeta(String tripId, String serviceDate) {
        if (tripId == null || tripId.isEmpty()) {
            return null;
        }
        
        // For cache key, include service date to handle same trip on different days
        String cacheKey = tripId + ":" + (serviceDate != null ? serviceDate : "unknown");
        TripMeta cached = tripMetaCache.get(cacheKey);
        if (cached != null) return cached;

        // Use provided service date or fall back to current date
        String startDate = serviceDate;
        if (startDate == null) {
            ZoneId zone = ZoneId.of("Europe/Paris");
            LocalDate date = LocalDate.now(zone);
            startDate = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }

        String sql = "SELECT t.trip_id, t.route_id, t.direction_id, MIN(COALESCE(st.departure_timestamp, st.arrival_timestamp)) AS first_time, MAX(COALESCE(st.arrival_timestamp, st.departure_timestamp)) AS last_time " +
                "FROM trips t JOIN stop_times st ON st.trip_id = t.trip_id WHERE t.trip_id = ? GROUP BY t.trip_id, t.route_id, t.direction_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tripId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String routeId = rs.getString("route_id");
                    int dir = rs.getInt(COL_DIRECTION_ID);
                    int first = rs.getInt("first_time");
                    int last = rs.getInt("last_time");
                    TripMeta meta = new TripMeta(tripId, routeId, dir, first, last, startDate);
                    tripMetaCache.put(cacheKey, meta);
                    return meta;
                }
            }
        } catch (SQLException e) {
            logger.debug("Error getting trip metadata for trip: {}", tripId, e);
        }
        return null;
    }
    
    /**
     * Returns trip metadata for a given trip ID using current service date.
     * 
     * @param tripId The trip identifier
     * @return TripMeta object or null if not found
     */
    public static TripMeta getTripMeta(String tripId) {
        return getTripMeta(tripId, null);
    }

    /**
     * Returns the first stop time (in seconds since midnight) for a given trip.
     * 
     * <p>This method retrieves the earliest arrival or departure time from the GTFS stop_times
     * table for the specified trip. The time is returned as seconds since midnight (00:00:00).
     * For trips that cross midnight, times can be >= 86400 (24:00:00 or later).
     * 
     * @param tripId The trip identifier
     * @return The first stop time in seconds since midnight, or null if not found
     */
    public static Integer getFirstStopTime(String tripId) {
        if (tripId == null || tripId.isEmpty()) {
            return null;
        }

        String sql = "SELECT MIN(COALESCE(departure_timestamp, arrival_timestamp)) AS first_time " +
                "FROM stop_times WHERE trip_id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tripId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int firstTime = rs.getInt("first_time");
                    if (!rs.wasNull()) {
                        return firstTime;
                    }
                }
            }
        } catch (SQLException e) {
            logger.debug("Error getting first stop time for trip: {}", tripId, e);
        }
        return null;
    }

    /**
     * Returns the last stop time (seconds since midnight) for the given trip.
     * For trips that cross midnight, times can be >= 86400 (24:00:00 or later).
     *
     * @param tripId The trip identifier
     * @return The last stop time in seconds since midnight, or null if not found
     */
    public static Integer getLastStopTime(String tripId) {
        if (tripId == null || tripId.isEmpty()) {
            return null;
        }

        String sql = "SELECT MAX(COALESCE(arrival_timestamp, departure_timestamp)) AS last_time " +
                "FROM stop_times WHERE trip_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tripId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int lastTime = rs.getInt("last_time");
                    if (!rs.wasNull()) {
                        return lastTime;
                    }
                }
            }
        } catch (SQLException e) {
            logger.debug("Error getting last stop time for trip: {}", tripId, e);
        }
        return null;
    }
}