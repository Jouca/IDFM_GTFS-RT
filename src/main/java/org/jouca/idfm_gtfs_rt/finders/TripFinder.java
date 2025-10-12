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

public class TripFinder {
    private static final String DB_URL = "jdbc:sqlite:./gtfs.db";
    private static final BasicDataSource dataSource = new BasicDataSource();

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
     * Lightweight container for trip metadata needed to build GTFS-RT TripDescriptor
     */
    public static class TripMeta {
        public final String tripId;
        public final String routeId;
        public final int directionId;
        /** first stop time in seconds since start of service day (0-86399) */
        public final int firstTimeSecOfDay;

        public TripMeta(String tripId, String routeId, int directionId, int firstTimeSecOfDay) {
            this.tripId = tripId;
            this.routeId = routeId;
            this.directionId = directionId;
            this.firstTimeSecOfDay = firstTimeSecOfDay;
        }
    }

    /**
     * Recherche le trip_id correspondant à une séquence d'EstimatedCall, en essayant de matcher les horaires et arrêts sur le plus proche possible.
     * Cette version cherche le trip_id dont les horaires théoriques sont les plus proches des horaires temps réel (EstimatedCall).
     * On considère le trip avec la somme des différences horaires la plus faible.
     * Ajout d'une fenêtre de recherche progressive sur les horaires (de -1/+1 à -5/+30 minutes).
     * Retourne le trip_id le plus proche, ou null si aucun ne correspond.
     */
    public static String findTripIdFromEstimatedCalls(
        String routeId,
        List<EstimatedCall> estimatedCalls,
        boolean isArrivalTime,
        String destinationId,
        String journeyNote,
        Integer directionId
    ) throws SQLException {
        if (routeId == null || estimatedCalls == null || estimatedCalls.isEmpty()) {
            throw new IllegalArgumentException("Inputs cannot be null or empty.");
        }

        String timeColumn = isArrivalTime ? "arrival_timestamp" : "departure_timestamp";
        ZoneId zone = ZoneId.of("Europe/Paris");

        Map<String, List<Integer>> stopTimes = new HashMap<>();
        LocalDate date = null;

        for (EstimatedCall ec : estimatedCalls) {
            Instant instant = Instant.parse(ec.isoTime());
            ZonedDateTime zdt = instant.atZone(zone);
            date = zdt.toLocalDate();

            int seconds = (int) (zdt.toEpochSecond() - date.atStartOfDay(zone).toEpochSecond());
            stopTimes.computeIfAbsent(ec.stopId(), k -> new ArrayList<>()).add(seconds);
        }

        String yyyymmdd = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        int dayOfWeek = date.getDayOfWeek().getValue();
        String weekday = String.valueOf(dayOfWeek == 7 ? 0 : dayOfWeek);

        List<String> allStopIds = estimatedCalls.stream().map(EstimatedCall::stopId).distinct().collect(Collectors.toList());

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
            SELECT st.trip_id, st.stop_id, st.stop_sequence, st.%s %% 86400 as stop_time
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            WHERE t.route_id = ?
            AND t.service_id IN (SELECT service_id FROM valid_services)
            AND t.service_id NOT IN (SELECT service_id FROM excluded_services)
        """.formatted(timeColumn));

        query.append("AND st.stop_id IN (")
             .append(allStopIds.stream().map(x -> "?").collect(Collectors.joining(",")))
             .append(")\n");

        // Add direction constraint if provided
        if (directionId != null) {
            query.append("AND t.direction_id = ?\n");
        }

        // Add destination constraint: last stop_id must match destinationId and be the last stop_sequence for the trip
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

        if (journeyNote != null && journeyNote.length() == 4) {
            query.append("AND t.trip_headsign = ?\n");
        }

        // Fenêtres de recherche en minutes (min, max)
        int[][] windows = {
            {-1, 1},
            {-2, 2},
            {-3, 5},
            {-3, 10},
            {-3, 20},
            {-3, 30}
        };

    Map<String, Map<String, List<Integer>>> tripStopTimes = new HashMap<>();

        // Build cache key for this query to avoid repeating heavy DB work for identical inputs
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(routeId).append('|').append(isArrivalTime ? 'A' : 'D').append('|').append(destinationId).append('|');
        if (journeyNote != null) keyBuilder.append(journeyNote).append('|');
        if (directionId != null) keyBuilder.append(directionId).append('|');
        for (EstimatedCall ec : estimatedCalls) {
            keyBuilder.append(ec.stopId()).append('@').append(ec.isoTime()).append('|');
        }
        String cacheKey = keyBuilder.toString();
        if (findTripCache.containsKey(cacheKey)) {
            return findTripCache.get(cacheKey);
        }

       try (Connection conn = dataSource.getConnection();
           PreparedStatement stmt = conn.prepareStatement(query.toString())) {

            int i = 1;
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, yyyymmdd);
            for (int j = 0; j < 7; j++) stmt.setString(i++, weekday);
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, yyyymmdd);
            stmt.setString(i++, routeId);

            for (String stopId : allStopIds) {
                stmt.setString(i++, stopId);
            }
            
            // Set directionId if provided
            if (directionId != null) {
                stmt.setInt(i++, directionId);
            }
            
            // Set destinationId for the EXISTS clause
            stmt.setString(i++, destinationId);

            // Set trip headsign for the filter
            if (journeyNote != null && journeyNote.length() == 4) {
                stmt.setString(i++, journeyNote);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tripId = rs.getString("trip_id");
                    String stopId = rs.getString("stop_id");
                    int stopTime = rs.getInt("stop_time");

                    tripStopTimes
                        .computeIfAbsent(tripId, k -> new HashMap<>())
                        .computeIfAbsent(stopId, k -> new ArrayList<>())
                        .add(stopTime);
                }
            }
        }

        List<TripMatch> allMatches = new ArrayList<>();

        for (int[] window : windows) {
            int minWindow = window[0] * 60;
            int maxWindow = window[1] * 60;

            // Pour chaque trip, calculer la somme des différences horaires
            for (Map.Entry<String, Map<String, List<Integer>>> entry : tripStopTimes.entrySet()) {
                String tripId = entry.getKey();
                Map<String, List<Integer>> tripStops = entry.getValue();

                long totalDiff = 0;
                boolean allStopsMatched = true;

                for (EstimatedCall ec : estimatedCalls) {
                    String stopId = ec.stopId();
                    Instant inst = Instant.parse(ec.isoTime());
                    int realTime = (int) (inst.atZone(zone).toEpochSecond() - date.atStartOfDay(zone).toEpochSecond());
                    // Keep realTime as seconds since service day's start; do not wrap modulo 86400

                    List<Integer> theoreticalTimes = tripStops.get(stopId);
                    if (theoreticalTimes == null || theoreticalTimes.isEmpty()) {
                        allStopsMatched = false;
                        break;
                    }
                    // Find the closest theoretical time for this stop within the window
                    Integer bestTheo = null;
                    int minDiff = Integer.MAX_VALUE;
                    for (Integer theoTime : theoreticalTimes) {
                        // theoTime may be >= 86400 for trips continuing after midnight; keep as-is
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
                    if (bestTheo == null) {
                        allStopsMatched = false;
                        break;
                    }
                    totalDiff += minDiff;
                }

                if (allStopsMatched) {
                    allMatches.add(new TripMatch(tripId, totalDiff));
                }
            }
        }

        // Trier tous les matches trouvés par totalDiff croissant
        allMatches.sort((a, b) -> Long.compare(a.totalDiff, b.totalDiff));

        if (!allMatches.isEmpty()) {
            // Retourner le trip_id avec la plus petite différence totale
            String result = allMatches.get(0).tripId;
            findTripCache.put(cacheKey, result);
            return result;
        }

        // Si aucun trip trouvé dans les fenêtres, retourner null
        findTripCache.put(cacheKey, null);
        return null;
    }

    // Classe utilitaire pour trier les matches
    private static class TripMatch {
        String tripId;
        long totalDiff;
        TripMatch(String tripId, long totalDiff) {
            this.tripId = tripId;
            this.totalDiff = totalDiff;
        }
    }

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
                    String stopId = rs.getString("stop_id");
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
                        String sId = rs.getString("stop_id");
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

    public static String findStopIdFromCode(String stopCode) {
        String query = "SELECT stop_id FROM stops WHERE stop_id LIKE ?;";
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, "%:" + stopCode);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("stop_id");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

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

    private static final String EMPTY_MARKER = "__NULL__";
    public static final Map<String, String> stopCodeCache = new ConcurrentHashMap<>();

    // Caches to reduce DB access during a processing run
    private static final ConcurrentHashMap<String, TripMeta> tripMetaCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<String>> allStopTimesCache = new ConcurrentHashMap<>();
    /** tripId -> (stopId -> list of sequences as strings) */
    private static final ConcurrentHashMap<String, Map<String, List<String>>> stopSequencesCache = new ConcurrentHashMap<>();
    
    // Simple LRU cache for findTripIdFromEstimatedCalls results to avoid repeated heavy DB queries
    private static final int FIND_TRIP_CACHE_SIZE = 5000;
    private static final java.util.Map<String, String> findTripCache = java.util.Collections.synchronizedMap(
        new java.util.LinkedHashMap<String, String>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(java.util.Map.Entry<String, String> eldest) {
                return size() > FIND_TRIP_CACHE_SIZE;
            }
        }
    );


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
     * Retourne l'heure d'arrivée théorique (epoch seconds) pour un stop donné dans stopTimes.
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
     * Retourne l'heure de départ théorique (epoch seconds) pour un stop donné dans stopTimes.
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

    public static String getTripDirection(String tripId) {
        TripMeta cached = tripMetaCache.get(tripId);
        if (cached != null) return String.valueOf(cached.directionId);

        String query = "SELECT direction_id FROM trips WHERE trip_id = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, tripId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String dir = rs.getString("direction_id");
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
     * Returns all trips (with minimal metadata) that are active today for the given route ids.
     * This uses calendar and calendar_dates to determine service validity and aggregates the
     * earliest time (arrival or departure) for each trip to allow filtering by time window.
     */
    public static List<TripMeta> getActiveTripsForRoutesToday(List<String> routeIds) {
        if (routeIds == null || routeIds.isEmpty()) return java.util.Collections.emptyList();

        ZoneId zone = ZoneId.of("Europe/Paris");
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
                    int dir = rs.getInt("direction_id");
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
     * Returns TripMeta for a given trip id, including routeId, directionId and the trip's first time of day.
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
                    int dir = rs.getInt("direction_id");
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