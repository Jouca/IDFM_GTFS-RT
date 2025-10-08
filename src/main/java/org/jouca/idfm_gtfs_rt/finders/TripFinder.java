package org.jouca.idfm_gtfs_rt.finders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.jouca.idfm_gtfs_rt.records.EstimatedCall;

public class TripFinder {
    private static final String DB_URL = "jdbc:sqlite:./gtfs.db";
    private static final BasicDataSource dataSource = new BasicDataSource();

    static {
        dataSource.setUrl(DB_URL);
        dataSource.setMinIdle(4); // Increase min idle connections
        dataSource.setMaxIdle(10); // Increase max idle connections
        dataSource.setMaxTotal(20); // Allow more total connections
        dataSource.setMaxOpenPreparedStatements(50); // Allow more prepared statements
        dataSource.setInitialSize(4); // Pre-initialize connections
        dataSource.setPoolPreparedStatements(true); // Enable prepared statement pooling
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
        String journeyNote
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
                    realTime = ((realTime % 86400) + 86400) % 86400; // Normalize to 0-86399

                    List<Integer> theoreticalTimes = tripStops.get(stopId);
                    if (theoreticalTimes == null || theoreticalTimes.isEmpty()) {
                        allStopsMatched = false;
                        break;
                    }
                    // Find the closest theoretical time for this stop within the window
                    Integer bestTheo = null;
                    int minDiff = Integer.MAX_VALUE;
                    for (Integer theoTime : theoreticalTimes) {
                        int normalizedTheoTime = ((theoTime % 86400) + 86400) % 86400; // Normalize to 0-86399
                        int diff = normalizedTheoTime - realTime;

                        // Handle wrap-around at midnight (difference should be in [-43200, 43200])
                        if (diff > 43200) diff -= 86400;
                        if (diff < -43200) diff += 86400;

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
            return allMatches.get(0).tripId;
        }

        // Si aucun trip trouvé dans les fenêtres, retourner null
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
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return results;
    }

    public static String findStopSequence(String tripId, String stopId, List<String> stopUpdates) {
        String query = """
            SELECT st.stop_sequence
            FROM stop_times st
            WHERE st.trip_id = ? AND st.stop_id = ?
            AND st.stop_sequence NOT IN (%s)
            LIMIT 1;
        """;

        // Build the NOT IN clause dynamically
        StringBuilder notInClause = new StringBuilder();
        for (int i = 0; i < stopUpdates.size(); i++) {
            notInClause.append("?");
            if (i < stopUpdates.size() - 1) {
                notInClause.append(",");
            }
        }

        query = query.replace("%s", notInClause.toString());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, tripId);
            stmt.setString(2, stopId);

            int index = 3;
            for (String stopUpdate : stopUpdates) {
                stmt.setString(index++, stopUpdate);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("stop_sequence");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
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

    public static final Map<String, String> stopCodeCache = new HashMap<>();

    public static String resolveStopId(String stopCode) {
        if (stopCodeCache.containsKey(stopCode)) {
            return stopCodeCache.get(stopCode);
        }
        String stopId = TripFinder.findStopIdFromCode(stopCode);
        if (stopId == null) {
            stopId = TripFinder.findStopIdFromStopExtension(stopCode);
        }
        stopCodeCache.put(stopCode, stopId);
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
                    return serviceDayEpoch + (Long.parseLong(arrivalTimeCollected) % 86400);
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
                    return serviceDayEpoch + (Long.parseLong(departureTimeCollected) % 86400);
                }
            }
        }
        return null;
    }

    public static String getTripDirection(String tripId) {
        String query = "SELECT direction_id FROM trips WHERE trip_id = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setString(1, tripId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("direction_id");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}