package org.jouca.idfm_gtfs_rt.generator;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
import org.jouca.idfm_gtfs_rt.records.StopClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.transit.realtime.GtfsRealtime;

/**
 * Generator for GTFS Realtime TripUpdate feeds from SIRI Lite data.
 * 
 * <p>
 * This component is responsible for:
 * <ul>
 * <li>Fetching real-time transit data from SIRI Lite API</li>
 * <li>Matching real-time data with theoretical GTFS trips</li>
 * <li>Generating GTFS-RT TripUpdate messages with stop time predictions</li>
 * <li>Detecting and marking canceled trips</li>
 * <li>Managing vehicle-to-trip associations across updates</li>
 * </ul>
 * 
 * <p>
 * The generator uses parallel processing to handle large volumes of real-time
 * data
 * efficiently and maintains state to track vehicle assignments across feed
 * updates.
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

    /**
     * Provides the stop-level disruption closures merged into TripUpdates as SKIPPED.
     * May be null when this class is instantiated outside of the Spring context (e.g. in unit
     * tests exercising other methods directly), in which case disruption merging is skipped.
     */
    @Autowired(required = false)
    private AlertGenerator alertGenerator;

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

    /** Status value indicating a missed stop in SIRI Lite data */
    private static final String STATUS_MISSED = "MISSED";

    /** SIRI Lite JSON field name for operator reference */
    private static final String FIELD_OPERATOR_REF = "OperatorRef";

    /** SIRI Lite JSON field name for departure stop assignment (contains ExpectedQuayRef) */
    private static final String FIELD_DEPARTURE_STOP_ASSIGNMENT = "DepartureStopAssignment";

    /** SIRI Lite JSON field name for arrival stop assignment (contains ExpectedQuayRef) */
    private static final String FIELD_ARRIVAL_STOP_ASSIGNMENT = "ArrivalStopAssignment";

    /** SIRI Lite JSON field name for the expected quay (platform) reference */
    private static final String FIELD_EXPECTED_QUAY_REF = "ExpectedQuayRef";

    /** SIRI Lite JSON field name for departure platform name */
    private static final String FIELD_DEPARTURE_PLATFORM_NAME = "DeparturePlatformName";

    /** SIRI Lite JSON field name for arrival platform name */
    private static final String FIELD_ARRIVAL_PLATFORM_NAME = "ArrivalPlatformName";

    /** Blacklist of operator IDs to exclude from GTFS-RT feed and trip matching */
    private static final Set<String> OPERATOR_BLACKLIST = Set.of(
        "MeC_Bus_PC:Operator:*",
        "RATP-SIV:*"
        // Add more operator IDs here as needed
    );

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
     * <p>
     * Trip state is keyed by stable theoretical trip ID to handle cases where
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
         * Epoch second of the trip's last stop (0 if not yet computed). Used for
         * expiry.
         */
        long lastStopEpoch;

        /**
         * Last observed delay in seconds (aimed vs expected time), used to re-emit the
         * trip
         * from theoretical schedule when SIRI data is temporarily absent.
         * Long.MIN_VALUE means no delay has been observed yet.
         */
        volatile long lastKnownDelaySeconds = Long.MIN_VALUE;

        /** Route ID of this trip, stored for cache-only re-emission. */
        String routeId;

        /** Direction ID of this trip, stored for cache-only re-emission. */
        int directionId;

        /**
         * Constructs a new TripState.
         *
         * @param tripId     the GTFS trip identifier
         * @param vehicleId  the real-time vehicle identifier
         * @param lastUpdate the timestamp of this update (epoch seconds)
         */
        TripState(String tripId, String vehicleId, long lastUpdate) {
            this.tripId = tripId;
            this.vehicleId = vehicleId;
            this.lastUpdate = lastUpdate;
            this.lastStopEpoch = 0;
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
     * Info about a partial (short-turn) blacklisted trip, exposed via
     * /partial-trips.
     *
     * @param syntheticTripId       the BL-prefixed ID used in the GTFS-RT feed
     * @param routeId               GTFS route ID
     * @param routeShortName        human-readable line name (e.g. "72")
     * @param directionId           0 or 1
     * @param theoreticalTerminus   stop ID of the full theoretical terminus
     * @param actualDestinationId   stop ID where this trip actually ends
     * @param actualDestinationName human-readable destination name from SIRI
     * @param stops                 ordered list of stop IDs in the emitted trip
     * @param generatedAt           epoch second when this entry was last written
     */
    public record PartialTripInfo(
        String syntheticTripId,
        String routeId,
        String routeShortName,
        int directionId,
        String theoreticalTerminus,
        String actualDestinationId,
        String actualDestinationName,
        List<String> stops,
        long generatedAt
    ) {}

    /** Latest partial trips detected in the most recent feed generation cycle. */
    public static final Map<String, PartialTripInfo> partialTrips = new ConcurrentHashMap<>();

    /**
     * Persistent cache for blacklisted-operator trip matches across generation
     * cycles.
     * Maps theoretical GTFS trip ID → last successful match info (delay, expiry
     * epoch).
     * Avoids re-running the expensive Phase-1 median-delay scan every 2 minutes for
     * trips that are already confirmed matched.
     */
    static class BlacklistedTripCache {
        final String tripId;
        /** Last confirmed median delay in seconds (used to seed Phase-2 directly). */
        volatile long medianDelay;
        /** Epoch second of the trip's last stop; entry is removed once this passes. */
        final long lastStopEpoch;
        /** Epoch second when this entry was last successfully updated. */
        volatile long lastUpdated;

        BlacklistedTripCache(String tripId, long medianDelay, long lastStopEpoch, long lastUpdated) {
            this.tripId = tripId;
            this.medianDelay = medianDelay;
            this.lastStopEpoch = lastStopEpoch;
            this.lastUpdated = lastUpdated;
        }
    }

    /** Cache of blacklisted trip matches, keyed by theoretical GTFS trip ID. */
    public static final Map<String, BlacklistedTripCache> blacklistedMatchCache = new ConcurrentHashMap<>();

    /** Result of matching a vehicle trajectory to a theoretical trip. */
    private record TrajectoryMatch(TripFinder.TripMeta tripMeta, long medianDelay) {}

    /**
     * A single real-time call flattened from SIRI data for grouping purposes.
     *
     * @param stopSeq         stop_sequence in the reference trip
     * @param stopId          GTFS stop_id resolved for this stop
     * @param arrivalEpoch    observed arrival epoch-seconds (Long.MIN_VALUE if
     *                        absent)
     * @param departureEpoch  observed departure epoch-seconds (Long.MIN_VALUE if
     *                        absent)
     * @param absoluteMinutes observed time in minutes from service-day midnight
     *                        (used for grouping)
     */
    private record CallData(int stopSeq, String stopId,
            long arrivalEpoch, long departureEpoch,
            double absoluteMinutes) {
    }

    /**
     * Reference-trip data used to convert SIRI stop codes to canonical
     * stop-sequence positions.
     * Derived once per line+direction from the longest active trip.
     *
     * @param codeToStopId          SIRI stop code → GTFS stop_id
     * @param stopIdToSeq           GTFS stop_id → stop_sequence
     * @param seqToStopId           stop_sequence → GTFS stop_id
     * @param seqToDepSecs          stop_sequence → departure seconds-of-day
     * @param seqToInterStopMinutes stop_sequence → theoretical travel time from
     *                              previous stop (minutes)
     */
    private record ReferenceStopData(
            Map<String, String> codeToStopId,
            Map<String, Integer> stopIdToSeq,
            Map<Integer, String> seqToStopId,
            Map<Integer, Long> seqToDepSecs,
            Map<Integer, Double> seqToInterStopMinutes) {
    }

    /**
     * Internal record to maintain the original processing order of entities.
     * Used to preserve the sorted order after parallel processing.
     */
    private record IndexedEntity(int index, GtfsRealtime.FeedEntity entity) {
    }

    /**
     * Main entry point for generating GTFS-RT feed.
     * 
     * <p>
     * This method orchestrates the entire feed generation process:
     * <ol>
     * <li>Fetches real-time SIRI Lite data from the transit agency API</li>
     * <li>Validates that required database tables exist</li>
     * <li>Creates a GTFS-RT FeedMessage with appropriate headers</li>
     * <li>Processes all real-time vehicle journeys in parallel</li>
     * <li>Identifies and marks canceled trips</li>
     * <li>Writes the completed feed to a Protocol Buffer file</li>
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

        // Check if object_codes_extension table exists in SQLite database
        if (!TripFinder.checkIfObjectCodesExtensionTableExists()) {
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

        // Merge stop-level disruption closures (e.g. construction) as SKIPPED, for trips
        // that wouldn't otherwise be touched by live SIRI-Lite data
        applyDisruptionStopClosures(feedMessage);

        // Build the feed once (may contain REPLACEMENT for blacklisted non-extra
        // journeys)
        GtfsRealtime.FeedMessage builtFeed = feedMessage.build();

        // Beta feed: keeps REPLACEMENT as-is
        writeFeedToFile(builtFeed, "gtfs-rt-trips-idfm-beta.pb");

        // Main feed: all REPLACEMENT converted to ADDED, then enriched with platform assignments
        GtfsRealtime.FeedMessage mainFeed = convertReplacementToAdded(builtFeed);
        generatePlatformFeed(mainFeed, siriLiteData);
    }

    /**
     * Merges stop-level disruption closures (e.g. a construction closure of specific stops on
     * a line) into the feed being built, as {@code SKIPPED} {@code StopTimeUpdate}s.
     * <p>
     * Unlike the SIRI-Lite-driven SKIPPED logic elsewhere in this class (which only fires when
     * a vehicle's live monitoring reports a call as missed/cancelled), this covers stops closed
     * by a planned disruption regardless of whether any live vehicle data exists for the trip —
     * without it, a disruption like a station closed for construction would show up in the
     * Alerts feed but nowhere in TripUpdates.
     * <p>
     * For every trip of an affected route scheduled today (or, before 8am, yesterday's overnight
     * trips — see {@link TripFinder#getActiveTripsForRoutesToday}), each scheduled visit to a
     * closed stop whose theoretical time falls within one of the disruption's active windows is
     * marked SKIPPED — including on a trip that already has a live SIRI-Lite entry for that exact
     * stop visit, since SIRI-Lite's per-call status has no notion of a planned disruption and
     * would otherwise keep predicting a normal arrival at a stop that is actually closed (see
     * {@link #markStopSkipped}).
     *
     * @param feedMessage the feed being built; mutated in place
     */
    private void applyDisruptionStopClosures(GtfsRealtime.FeedMessage.Builder feedMessage) {
        if (alertGenerator == null) {
            System.out.println("[Trips] Disruption stop closures skipped: AlertGenerator not wired.");
            return;
        }

        List<StopClosure> closures = alertGenerator.getActiveStopClosures();
        if (closures.isEmpty()) {
            System.out.println("[Trips] Disruption stop closures: none active.");
            return;
        }

        Map<String, List<StopClosure>> closuresByRoute = new HashMap<>();
        for (StopClosure closure : closures) {
            closuresByRoute.computeIfAbsent(closure.routeId(), k -> new ArrayList<>()).add(closure);
        }

        Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId = indexTripUpdatesByTripId(feedMessage);
        int entitiesBefore = feedMessage.getEntityCount();

        for (Map.Entry<String, List<StopClosure>> routeEntry : closuresByRoute.entrySet()) {
            List<TripFinder.TripMeta> trips = TripFinder.getActiveTripsForRoutesToday(List.of(routeEntry.getKey()));
            if (trips.isEmpty()) {
                continue;
            }

            for (TripFinder.TripMeta trip : trips) {
                List<String> stopTimeRows = TripFinder.getAllStopTimesFromTrip(trip.tripId);
                if (stopTimeRows.isEmpty()) {
                    continue;
                }

                long serviceDayStartEpoch = serviceDayStartEpoch(trip.startDate);

                for (StopClosure closure : routeEntry.getValue()) {
                    if (closure.entireRouteClosure()) {
                        markTripCanceledIfWithinWindow(feedMessage, tripUpdatesByTripId, trip, stopTimeRows,
                                serviceDayStartEpoch, closure.activePeriods());
                    }
                    for (String stopId : closure.stopIds()) {
                        markMatchingStopVisitsSkipped(feedMessage, tripUpdatesByTripId, trip, stopId,
                                stopTimeRows, serviceDayStartEpoch, closure.activePeriods());
                    }
                    for (StopClosure.Section section : closure.sections()) {
                        markSectionSkipped(feedMessage, tripUpdatesByTripId, trip, stopTimeRows,
                                serviceDayStartEpoch, section, closure.activePeriods());
                    }
                }
            }
        }

        System.out.println("[Trips] Disruption stop closures: " + closuresByRoute.size() + " route(s) affected, "
                + (feedMessage.getEntityCount() - entitiesBefore) + " new trip entities added.");
    }

    /**
     * Scans a trip's theoretical stop times for every visit to {@code stopId} whose scheduled
     * time falls within one of the given active windows, and marks each such visit SKIPPED.
     * A looping route can visit the same stop more than once; every matching visit is skipped,
     * not just the first.
     */
    private void markMatchingStopVisitsSkipped(GtfsRealtime.FeedMessage.Builder feedMessage,
            Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId, TripFinder.TripMeta trip,
            String stopId, List<String> stopTimeRows, long serviceDayStartEpoch,
            List<StopClosure.Window> activePeriods) {
        for (String row : stopTimeRows) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4 || !parts[0].equals(stopId)) {
                continue;
            }

            int stopSequence;
            try {
                stopSequence = Integer.parseInt(parts[3]);
            } catch (NumberFormatException e) {
                continue;
            }

            if (isConfirmedServedByLiveData(tripUpdatesByTripId, trip.tripId, stopId, stopSequence)) {
                continue;
            }

            long scheduledEpoch = parseSec(parts[1], serviceDayStartEpoch);
            if (scheduledEpoch == Long.MIN_VALUE) {
                scheduledEpoch = parseSec(parts[2], serviceDayStartEpoch);
            }
            scheduledEpoch = preferLiveEpoch(tripUpdatesByTripId, trip.tripId, stopId, stopSequence, scheduledEpoch);
            if (scheduledEpoch == Long.MIN_VALUE || !isWithinAnyWindow(scheduledEpoch, activePeriods)) {
                continue;
            }

            markStopSkipped(feedMessage, tripUpdatesByTripId, trip.tripId, trip.routeId, trip.startDate,
                    stopId, stopSequence);
        }
    }

    /**
     * Whether SIRI-Lite already has a live, non-cancelled call for this exact stop visit — i.e.
     * IDFM's own real-time vehicle tracking positively confirms the vehicle stops here normally.
     * <p>
     * Reproduces a real Ligne N case: a disruption named only "Clamart non desservie", but its
     * structured {@code impactedSections} boundary was Meudon → Vanves-Malakoff (the two stations
     * immediately either side of Clamart, used by IDFM merely to bracket the one closed station in
     * between — not because Meudon and Vanves-Malakoff themselves lack service). Blindly trusting
     * the section as inclusive marked those two neighbours SKIPPED too, even though live SIRI-Lite
     * data for every train on the line reported them {@code ON_TIME} throughout the closure. Since
     * IDFM doesn't expose whether a section's boundary is inclusive or exclusive, live data — the
     * operator's own real-time truth — is the tie-breaker: a stop it already confirms as normally
     * served must not be overridden to SKIPPED by our static-schedule-derived closure logic.
     */
    private boolean isConfirmedServedByLiveData(Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId,
            String tripId, String stopId, int stopSequence) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = tripUpdatesByTripId.get(tripId);
        if (tripUpdate == null) {
            return false;
        }

        for (int i = 0; i < tripUpdate.getStopTimeUpdateCount(); i++) {
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder existing = tripUpdate.getStopTimeUpdateBuilder(i);
            if (!stopId.equals(existing.getStopId()) || existing.getStopSequence() != stopSequence) {
                continue;
            }
            if (existing.getScheduleRelationship() == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED) {
                return false;
            }
            return (existing.hasArrival() && existing.getArrival().hasTime())
                    || (existing.hasDeparture() && existing.getDeparture().hasTime());
        }
        return false;
    }

    /**
     * Prefers a live SIRI-Lite-reported arrival/departure time over the static schedule's for the
     * disruption-window check, when the trip already has a non-SKIPPED live {@code StopTimeUpdate}
     * for this exact stop visit.
     * <p>
     * The static GTFS schedule can lag behind IDFM's actual current timetable (e.g. a published
     * schedule change not yet reflected in the static feed we last imported), so a stop's static
     * departure time can disagree with what the real vehicle is actually doing by tens of minutes.
     * Trusting the static time alone in that case can make a train that is genuinely running fine
     * today — SIRI-Lite already reports it {@code ON_TIME} — look like it falls inside a
     * disruption's active window when in reality it doesn't, and get wrongly marked SKIPPED.
     * Live data, when present, is a strictly better source of truth for "when does this vehicle
     * actually visit this stop" than the static schedule.
     *
     * @param staticEpoch the static-schedule-derived epoch to fall back to if there is no better
     *                    live time (e.g. because SIRI-Lite has no data for this trip at all —
     *                    the core case this whole disruption-closure mechanism exists for)
     * @return the live time if one is available, otherwise {@code staticEpoch} unchanged
     */
    private long preferLiveEpoch(Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId, String tripId,
            String stopId, int stopSequence, long staticEpoch) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = tripUpdatesByTripId.get(tripId);
        if (tripUpdate == null) {
            return staticEpoch;
        }

        for (int i = 0; i < tripUpdate.getStopTimeUpdateCount(); i++) {
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder existing = tripUpdate.getStopTimeUpdateBuilder(i);
            if (!stopId.equals(existing.getStopId()) || existing.getStopSequence() != stopSequence) {
                continue;
            }
            if (existing.getScheduleRelationship() == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED) {
                break;
            }
            if (existing.hasArrival() && existing.getArrival().hasTime()) {
                return existing.getArrival().getTime();
            }
            if (existing.hasDeparture() && existing.getDeparture().hasTime()) {
                return existing.getDeparture().getTime();
            }
            break;
        }
        return staticEpoch;
    }

    /**
     * Marks every stop a trip visits <em>strictly between</em> (excluding) the two boundary
     * stations of a "no service between X and Y" section as SKIPPED, for whichever visits fall
     * within one of the closure's active windows.
     * <p>
     * Unlike {@link #markMatchingStopVisitsSkipped}, this does not rely on IDFM's per-stop
     * {@code impactedObjects} list at all — for a severe disruption ("no service between X and Y,
     * delayed on the rest of the line") that list has been observed to enumerate most or all of
     * the line's stations, not just the closed ones, so trusting it would mark merely-delayed
     * stops as having no service. The section boundary, resolved to actual stop_ids via
     * {@link TripFinder#getStopIdsForParentStation}, is the reliable source of which stops this
     * trip actually has no service at.
     * <p>
     * The boundary stops {@code from} and {@code to} themselves are excluded. Across many real
     * disruptions on many different operators — a single closed stop bracketed by its two
     * immediate, still-served neighbours ("l'arrêt X n'est pas desservi"), or an explicit "les
     * arrêts situés entre X et Y ne sont plus desservis" (the stops located <em>between</em> X and
     * Y — French "entre" excludes its bounds) — {@code from}/{@code to} consistently name the
     * stops the disruption is anchored on, not stops that are themselves unserved; IDFM has no
     * separate structured way to say "and these two are also closed." Treating them as included
     * was tried and wrongly canceled genuinely-served neighbouring stops on every operator that
     * uses this pattern (RATP and several others) whenever no live SIRI-Lite data existed to
     * override it — which, in practice, is most of the time (see {@link #isConfirmedServedByLiveData}'s
     * javadoc for real coverage figures). A rarer, genuinely whole-corridor closure that happens to
     * also affect its own named endpoints will still have every interior stop correctly marked;
     * only the two endpoint stations themselves are left alone absent live confirmation otherwise
     * — a small precision loss on two stations, far outweighed by fixing the far more common case.
     * <p>
     * If a trip pattern doesn't pass through both boundary stations (e.g. a different branch or
     * a short-turn service), the section simply doesn't apply to it. On a station visited more
     * than once by one trip (a looping route), the first (lowest stop_sequence) visit to each
     * boundary is used — section-based closures are only known to occur on linear lines in
     * practice.
     * <p>
     * A closure often only affects one direction of travel (e.g. a one-way detour around
     * roadworks) — IDFM does not expose this as a structured field, but {@code from}/{@code to}
     * are consistently ordered to match the affected direction's own travel order. So a section
     * only applies to a trip that visits {@code from} <em>before</em> {@code to} in its own
     * stop_sequence; a trip running the opposite way (which would visit {@code to} first) is
     * simply not affected by this section, even though it passes through the same two stations.
     * When both directions are genuinely closed, IDFM gives two reciprocal {@code Section}
     * entries (one per direction), so each real direction is still matched correctly this way.
     */
    private void markSectionSkipped(GtfsRealtime.FeedMessage.Builder feedMessage,
            Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId, TripFinder.TripMeta trip,
            List<String> stopTimeRows, long serviceDayStartEpoch, StopClosure.Section section,
            List<StopClosure.Window> activePeriods) {
        Set<String> fromStopIds = TripFinder.getStopIdsForParentStation(section.fromParentStationId());
        Set<String> toStopIds = TripFinder.getStopIdsForParentStation(section.toParentStationId());

        Integer fromSequence = null;
        Integer toSequence = null;
        for (String row : stopTimeRows) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4) {
                continue;
            }
            int seq;
            try {
                seq = Integer.parseInt(parts[3]);
            } catch (NumberFormatException e) {
                continue;
            }
            if (fromSequence == null && fromStopIds.contains(parts[0])) {
                fromSequence = seq;
            }
            if (toSequence == null && toStopIds.contains(parts[0])) {
                toSequence = seq;
            }
        }

        if (fromSequence == null || toSequence == null || fromSequence >= toSequence) {
            // Either this trip's pattern doesn't reach both boundaries, or it visits them in the
            // reverse order — meaning it runs in the direction NOT affected by this section (see
            // the direction-matching note in this method's javadoc).
            return;
        }

        int lo = fromSequence;
        int hi = toSequence;

        for (String row : stopTimeRows) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4) {
                continue;
            }
            int seq;
            try {
                seq = Integer.parseInt(parts[3]);
            } catch (NumberFormatException e) {
                continue;
            }
            if (seq <= lo || seq >= hi) {
                // The section's own boundary stops are excluded — see this method's javadoc for
                // why IDFM's from/to consistently name the two still-served stops bracketing the
                // actually-closed one(s), not endpoints that are themselves unserved.
                continue;
            }

            if (isConfirmedServedByLiveData(tripUpdatesByTripId, trip.tripId, parts[0], seq)) {
                continue;
            }

            long scheduledEpoch = parseSec(parts[1], serviceDayStartEpoch);
            if (scheduledEpoch == Long.MIN_VALUE) {
                scheduledEpoch = parseSec(parts[2], serviceDayStartEpoch);
            }
            scheduledEpoch = preferLiveEpoch(tripUpdatesByTripId, trip.tripId, parts[0], seq, scheduledEpoch);
            if (scheduledEpoch == Long.MIN_VALUE || !isWithinAnyWindow(scheduledEpoch, activePeriods)) {
                continue;
            }

            markStopSkipped(feedMessage, tripUpdatesByTripId, trip.tripId, trip.routeId, trip.startDate,
                    parts[0], seq);
        }
    }

    /**
     * Cancels a trip outright if any part of its theoretical schedule overlaps one of the given
     * active windows, for a disruption that closes a route entirely (e.g. a full-line closure for
     * maintenance work) with no specific stops or sections named.
     * <p>
     * Unlike {@link #markMatchingStopVisitsSkipped}/{@link #markSectionSkipped}, which skip
     * individual stop visits, this marks the whole {@code TripUpdate} as
     * {@code ScheduleRelationship.CANCELED} — appropriate here since no part of the route has
     * service, so no vehicle runs this trip at all.
     *
     * @param stopTimeRows   the trip's theoretical stop_times rows, used only to find the trip's
     *                       first/last scheduled times
     * @param activePeriods  the disruption's active time windows
     */
    private void markTripCanceledIfWithinWindow(GtfsRealtime.FeedMessage.Builder feedMessage,
            Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId, TripFinder.TripMeta trip,
            List<String> stopTimeRows, long serviceDayStartEpoch, List<StopClosure.Window> activePeriods) {
        long tripStartEpoch = Long.MAX_VALUE;
        long tripEndEpoch = Long.MIN_VALUE;

        for (String row : stopTimeRows) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4) {
                continue;
            }
            long arrival = parseSec(parts[1], serviceDayStartEpoch);
            long departure = parseSec(parts[2], serviceDayStartEpoch);
            long earliest = arrival != Long.MIN_VALUE ? arrival : departure;
            long latest = departure != Long.MIN_VALUE ? departure : arrival;
            if (earliest != Long.MIN_VALUE) {
                tripStartEpoch = Math.min(tripStartEpoch, earliest);
            }
            if (latest != Long.MIN_VALUE) {
                tripEndEpoch = Math.max(tripEndEpoch, latest);
            }
        }

        if (tripStartEpoch == Long.MAX_VALUE) {
            return;
        }
        if (tripEndEpoch == Long.MIN_VALUE) {
            tripEndEpoch = tripStartEpoch;
        }

        boolean overlapsClosure = false;
        for (StopClosure.Window window : activePeriods) {
            if (tripStartEpoch <= window.endEpochSec() && tripEndEpoch >= window.startEpochSec()) {
                overlapsClosure = true;
                break;
            }
        }
        if (!overlapsClosure) {
            return;
        }

        markTripCanceled(feedMessage, tripUpdatesByTripId, trip.tripId, trip.routeId, trip.startDate);
    }

    /**
     * Marks a trip's {@code TripUpdate} as {@code ScheduleRelationship.CANCELED}, reusing one
     * already in the feed if present (clearing any stop_time_updates it carries, which have no
     * meaning on a canceled trip) or creating a minimal one otherwise.
     */
    private void markTripCanceled(GtfsRealtime.FeedMessage.Builder feedMessage,
            Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId, String tripId, String routeId,
            String startDate) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = tripUpdatesByTripId.get(tripId);
        if (tripUpdate == null) {
            GtfsRealtime.FeedEntity.Builder entityBuilder = feedMessage.addEntityBuilder().setId(tripId);
            tripUpdate = entityBuilder.getTripUpdateBuilder();
            tripUpdatesByTripId.put(tripId, tripUpdate);
        }

        GtfsRealtime.TripDescriptor.Builder tripDescriptor = tripUpdate.getTripBuilder()
                .setTripId(tripId)
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        if (routeId != null && !routeId.isEmpty()) {
            tripDescriptor.setRouteId(routeId);
        }
        if (startDate != null && !startDate.isEmpty()) {
            tripDescriptor.setStartDate(startDate);
        }

        tripUpdate.clearStopTimeUpdate();
    }

    /**
     * Resolves the Paris-timezone service day start (midnight) as epoch seconds for a GTFS
     * {@code start_date} (YYYYMMDD), falling back to today if the date is missing or malformed.
     */
    private long serviceDayStartEpoch(String yyyymmdd) {
        try {
            LocalDate svcDay = (yyyymmdd != null && !yyyymmdd.isEmpty())
                    ? LocalDate.parse(yyyymmdd, DateTimeFormatter.ofPattern("yyyyMMdd"))
                    : LocalDate.now(ZONE_ID);
            return svcDay.atStartOfDay(ZONE_ID).toEpochSecond();
        } catch (Exception e) {
            return LocalDate.now(ZONE_ID).atStartOfDay(ZONE_ID).toEpochSecond();
        }
    }

    private boolean isWithinAnyWindow(long epochSec, List<StopClosure.Window> windows) {
        for (StopClosure.Window window : windows) {
            if (window.contains(epochSec)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds a tripId -&gt; TripUpdate.Builder index for every TripUpdate entity already present
     * in the feed being built, so disruption-derived closures can be merged into an existing
     * entity instead of creating a duplicate one.
     */
    private Map<String, GtfsRealtime.TripUpdate.Builder> indexTripUpdatesByTripId(
            GtfsRealtime.FeedMessage.Builder feedMessage) {
        Map<String, GtfsRealtime.TripUpdate.Builder> index = new HashMap<>();
        for (GtfsRealtime.FeedEntity.Builder entityBuilder : feedMessage.getEntityBuilderList()) {
            if (!entityBuilder.hasTripUpdate()) {
                continue;
            }
            GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
            String tripId = tripUpdate.getTrip().getTripId();
            if (tripId != null && !tripId.isEmpty()) {
                index.put(tripId, tripUpdate);
            }
        }
        return index;
    }

    /**
     * Marks a single stop visit as SKIPPED, on the TripUpdate for the given trip — reusing one
     * already in the feed if present, or creating a minimal SCHEDULED one otherwise so the
     * closure is reflected even for trips with no live SIRI-Lite data at all.
     * <p>
     * IDFM's live SIRI-Lite vehicle-monitoring data does not know about planned disruptions: a
     * stop closed for months by construction still gets an ordinary predicted arrival/departure
     * from SIRI-Lite, because the per-call status only reflects what the vehicle itself reports.
     * So an existing StopTimeUpdate is generally converted to SKIPPED in place rather than left
     * alone, clearing its arrival/departure predictions (the GTFS-Realtime spec doesn't expect a
     * SKIPPED stop_time_update to carry them) — unless it is already SKIPPED, in which case there
     * is nothing to do.
     * <p>
     * Matching prefers an exact (stop_sequence, stop_id) pair — the correct, loop-safe way to
     * identify one specific visit to a stop that a trip serves more than once. If that fails,
     * it falls back to matching by stop_id alone anywhere in the trip: a small number of live
     * SIRI-Lite entities have a stop_sequence that has drifted from the static GTFS numbering for
     * a given stop (a pre-existing trip-matching quirk unrelated to disruptions), and trusting
     * stop_sequence alone in that case would mark the wrong stop as skipped.
     */
    private void markStopSkipped(GtfsRealtime.FeedMessage.Builder feedMessage,
            Map<String, GtfsRealtime.TripUpdate.Builder> tripUpdatesByTripId, String tripId, String routeId,
            String startDate, String stopId, int stopSequence) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = tripUpdatesByTripId.get(tripId);
        if (tripUpdate == null) {
            GtfsRealtime.FeedEntity.Builder entityBuilder = feedMessage.addEntityBuilder().setId(tripId);
            tripUpdate = entityBuilder.getTripUpdateBuilder();
            GtfsRealtime.TripDescriptor.Builder tripDescriptor = tripUpdate.getTripBuilder()
                    .setTripId(tripId)
                    .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED);
            if (routeId != null && !routeId.isEmpty()) {
                tripDescriptor.setRouteId(routeId);
            }
            if (startDate != null && !startDate.isEmpty()) {
                tripDescriptor.setStartDate(startDate);
            }
            tripUpdatesByTripId.put(tripId, tripUpdate);
        }

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder fallbackByStopId = null;
        boolean stopIdAlreadyCovered = false;
        int insertIndex = tripUpdate.getStopTimeUpdateCount();
        boolean insertIndexFound = false;

        for (int i = 0; i < tripUpdate.getStopTimeUpdateCount(); i++) {
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder existing = tripUpdate.getStopTimeUpdateBuilder(i);
            int existingSequence = existing.getStopSequence();
            boolean sameStop = stopId.equals(existing.getStopId());

            if (sameStop && existingSequence == stopSequence) {
                convertToSkipped(existing);
                return;
            }
            if (sameStop) {
                stopIdAlreadyCovered = true;
                if (fallbackByStopId == null
                        && existing.getScheduleRelationship()
                                != GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED) {
                    fallbackByStopId = existing;
                }
            }
            if (!insertIndexFound && existingSequence > stopSequence) {
                insertIndex = i;
                insertIndexFound = true;
            }
        }

        if (fallbackByStopId != null) {
            convertToSkipped(fallbackByStopId);
            return;
        }
        if (stopIdAlreadyCovered) {
            // Every occurrence of this stop_id in the trip is already SKIPPED.
            return;
        }

        tripUpdate.addStopTimeUpdate(insertIndex, GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                .setStopSequence(stopSequence)
                .setStopId(stopId)
                .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED)
                .build());
    }

    /**
     * Sets a StopTimeUpdate to SKIPPED and clears its arrival/departure predictions, unless it
     * is already SKIPPED.
     */
    private void convertToSkipped(GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stu) {
        if (stu.getScheduleRelationship() != GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED) {
            stu.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
            stu.clearArrival();
            stu.clearDeparture();
        }
    }

    /**
     * Saves SIRI Lite data to a JSON file for debugging purposes.
     * 
     * @param siriLiteData the JSON node containing SIRI Lite data
     * @param filePath     the output file path
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
     * <p>
     * This method performs the following operations:
     * <ul>
     * <li>Extracts EstimatedVehicleJourney entities from SIRI Lite data</li>
     * <li>Sorts entities by earliest departure/arrival time</li>
     * <li>Collects current vehicle IDs from SIRI data</li>
     * <li>Cleans up vehicle-to-trip cache for vehicles not present in current SIRI
     * data</li>
     * <li>Processes entities in parallel using a thread pool</li>
     * <li>Maintains original order in the output feed</li>
     * <li>Cleans up stale trip states (older than 15 minutes)</li>
     * <li>Optionally exports debug information</li>
     * </ul>
     * 
     * @param siriLiteData the JSON data from SIRI Lite API
     * @param feedMessage  the GTFS-RT feed message builder to populate
     */
    private void processSiriLiteData(JsonNode siriLiteData, GtfsRealtime.FeedMessage.Builder feedMessage) {
        partialTrips.clear();

        List<JsonNode> entities = extractEntitiesFromSiriLite(siriLiteData);
        System.out.println(entities.size() + " entities found in SiriLite data.");

        // Separate blacklisted from normal entities
        List<JsonNode> normalEntities = new ArrayList<>();
        List<JsonNode> blacklistedEntities = new ArrayList<>();
        for (JsonNode entity : entities) {
            if (isOperatorBlacklisted(entity)) {
                blacklistedEntities.add(entity);
            } else {
                normalEntities.add(entity);
            }
        }

        sortEntitiesByTime(normalEntities);
        System.out.println("Processing " + normalEntities.size() + " normal entities and "
                + blacklistedEntities.size() + " blacklisted entities...");

        // Vehicle-to-trip cache cleanup only concerns normal (non-blacklisted) vehicles
        Set<String> currentVehicleIds = extractVehicleIds(normalEntities);
        cleanupAbsentVehicles(currentVehicleIds);

        Map<String, JsonNode> entitiesTrips = dumpDebugFiles ? new ConcurrentHashMap<>() : null;

        // Process normal entities in parallel with the standard matching algorithm
        List<IndexedEntity> builtEntities = new ArrayList<>(
                processEntitiesInParallel(normalEntities, entitiesTrips));

        // Re-emit cached normal trips that are still active but absent from this SIRI
        // cycle
        Set<String> emittedTripIds = builtEntities.stream()
                .map(e -> e.entity().getId())
                .collect(Collectors.toSet());
        reemitCachedNormalTrips(builtEntities, emittedTripIds);

        // Process all blacklisted entities together, grouped by line + direction so
        // that
        // calls spread across multiple EstimatedVehicleJourney objects are pooled and
        // redistributed to the correct theoretical GTFS trips.
        List<IndexedEntity> blacklistedBuilt = processAllBlacklistedEntities(
                blacklistedEntities, entitiesTrips, normalEntities.size());

        builtEntities.addAll(blacklistedBuilt);

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
     * Extracts vehicle IDs from SIRI Lite entities.
     * 
     * @param entities the list of SIRI Lite entities
     * @return set of vehicle IDs present in the current SIRI data
     */
    private Set<String> extractVehicleIds(List<JsonNode> entities) {
        return entities.stream()
                .filter(entity -> entity.has("DatedVehicleJourneyRef"))
                .map(entity -> entity.get("DatedVehicleJourneyRef").get(FIELD_VALUE).asText())
                .collect(Collectors.toSet());
    }

    /**
     * Cleans up vehicle-to-trip cache for vehicles that are no longer present in
     * SIRI data.
     * This ensures that when a vehicle disappears from SIRI, it will be removed
     * from the cache
     * and will need to go through trip matching again if it reappears with a
     * different trip.
     * 
     * @param currentVehicleIds the set of vehicle IDs present in the current SIRI
     *                          data
     */
    private void cleanupAbsentVehicles(Set<String> currentVehicleIds) {
        vehicleToTrip.keySet().removeIf(vehicleId -> !currentVehicleIds.contains(vehicleId));
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
     * @return epoch seconds of the earliest time, or Long.MAX_VALUE if no time
     *         available
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
     * @param entities      the list of entities to process
     * @param entitiesTrips optional map for debug output
     * @return list of successfully processed indexed entities
     */
    private List<IndexedEntity> processEntitiesInParallel(List<JsonNode> entities,
            Map<String, JsonNode> entitiesTrips) {
        int total = entities.size();
        renderProgressBar(0, total);

        ExecutorService executor = Executors
                .newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
        List<IndexedEntity> builtEntities = new ArrayList<>();
        try {
            List<Future<IndexedEntity>> futures = submitEntityProcessingTasks(entities, entitiesTrips, executor);
            builtEntities = collectFutureResults(futures, total);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted during parallel processing of {} SIRI Lite entities: {}", total,
                    e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.error("Execution error during parallel processing of {} SIRI Lite entities: {}", total,
                    e.getMessage(), e);
        } finally {
            shutdownExecutor(executor);
        }
        return builtEntities;
    }

    /**
     * Submits entity processing tasks to the executor.
     * 
     * @param entities      the list of entities to process
     * @param entitiesTrips optional map for debug output
     * @param executor      the executor service
     * @return list of futures for the submitted tasks
     */
    private List<Future<IndexedEntity>> submitEntityProcessingTasks(List<JsonNode> entities,
            Map<String, JsonNode> entitiesTrips, ExecutorService executor) {
        List<Future<IndexedEntity>> futures = new ArrayList<>(entities.size());
        for (int idx = 0; idx < entities.size(); idx++) {
            final int index = idx;
            final JsonNode entity = entities.get(idx);
            futures.add(executor
                    .submit((Callable<IndexedEntity>) () -> processEntity(entity, index, entitiesTrips)));
        }
        return futures;
    }

    /**
     * Adds processed entities to the GTFS-RT feed in sorted order.
     * 
     * @param builtEntities the list of indexed entities
     * @param feedMessage   the feed message builder
     */
    private void addEntitiesToFeed(List<IndexedEntity> builtEntities, GtfsRealtime.FeedMessage.Builder feedMessage) {
        // Add all entities to feed in sorted order
        builtEntities.stream()
                .sorted(Comparator.comparingInt(IndexedEntity::index))
                .forEach(indexed -> feedMessage.addEntity(indexed.entity()));
    }

    /**
     * Re-emits normal (non-blacklisted) trips that are present in the trip state
     * cache but
     * absent from the current SIRI cycle. Uses the last observed anchor delay
     * propagated
     * uniformly over the theoretical stop sequence.
     */
    private void reemitCachedNormalTrips(List<IndexedEntity> builtEntities, Set<String> emittedTripIds) {
        int startIndex = builtEntities.size();
        int reemitted = 0;
        for (Map.Entry<String, TripState> entry : tripStates.entrySet()) {
            String tripId = entry.getKey();
            TripState state = entry.getValue();

            // Skip trips already emitted from live SIRI data this cycle
            if (emittedTripIds.contains(tripId))
                continue;

            // Only re-emit if we have a delay to propagate
            if (state.lastKnownDelaySeconds == Long.MIN_VALUE)
                continue;

            TripFinder.TripMeta tripMeta = TripFinder.getTripMeta(tripId, null);
            if (tripMeta == null)
                continue;

            // Build entity using theoretical stops + constant last-known delay
            GtfsRealtime.FeedEntity entity = buildCachedNormalTripEntity(tripId, state, tripMeta);
            if (entity == null)
                continue;

            builtEntities.add(new IndexedEntity(startIndex + reemitted, entity));
            reemitted++;
        }
        if (reemitted > 0) {
            System.out.println("Re-emitted " + reemitted + " cached normal trips absent from SIRI this cycle.");
        }
    }

    /**
     * Builds a GTFS-RT entity for a normal trip using its theoretical schedule
     * shifted
     * by the last observed anchor delay. Used when the trip is in the state cache
     * but
     * absent from the current SIRI data.
     */
    private GtfsRealtime.FeedEntity buildCachedNormalTripEntity(String tripId, TripState state,
            TripFinder.TripMeta tripMeta) {
        List<String> allTheoreticalStops = TripFinder.getAllStopTimesFromTrip(tripId);
        if (allTheoreticalStops.isEmpty())
            return null;

        long serviceDayStartEpoch;
        try {
            java.time.LocalDate svcDay = (tripMeta.startDate != null && !tripMeta.startDate.isEmpty())
                    ? java.time.LocalDate.parse(tripMeta.startDate,
                            java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))
                    : java.time.LocalDate.now(ZONE_ID);
            serviceDayStartEpoch = svcDay.atStartOfDay(ZONE_ID).toEpochSecond();
        } catch (Exception e) {
            serviceDayStartEpoch = java.time.LocalDate.now(ZONE_ID).atStartOfDay(ZONE_ID).toEpochSecond();
        }

        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        String routeId = tripMeta.routeId != null ? tripMeta.routeId : (state.routeId != null ? state.routeId : "");
        tripUpdate.getTripBuilder()
                .setTripId(tripId)
                .setRouteId(routeId)
                .setDirectionId(tripMeta.directionId);
        if (tripMeta.startDate != null && !tripMeta.startDate.isEmpty()) {
            tripUpdate.getTripBuilder().setStartDate(tripMeta.startDate);
        }
        tripUpdate.getVehicleBuilder().setId(state.vehicleId);

        int seq = 1;
        for (String row : allTheoreticalStops) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4)
                continue;
            String stopId = parts[0];
            long theoreticalArr = parseSec(parts[1], serviceDayStartEpoch);
            long theoreticalDep = parseSec(parts[2], serviceDayStartEpoch);

            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stu = tripUpdate.addStopTimeUpdateBuilder();
            stu.setStopSequence(seq++);
            stu.setStopId(stopId);

            if (theoreticalArr != Long.MIN_VALUE) {
                stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                        .setTime(theoreticalArr + state.lastKnownDelaySeconds).build());
            }
            if (theoreticalDep != Long.MIN_VALUE) {
                stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                        .setTime(theoreticalDep + state.lastKnownDelaySeconds).build());
            }
        }

        if (tripUpdate.getStopTimeUpdateCount() == 0)
            return null;
        return entityBuilder.build();
    }

    /**
     * Builds a GTFS-RT SCHEDULED entity for a blacklisted trip using its
     * theoretical
     * schedule shifted by the last cached median delay. Used when SIRI data is
     * temporarily
     * absent for a trip that was successfully matched in a previous cycle.
     */
    private GtfsRealtime.FeedEntity buildCachedBlacklistedTripEntity(
            TripFinder.TripMeta tripMeta, Integer directionId, String lineId,
            long serviceDayStartEpoch, long medianDelay) {

        List<String> allTheoreticalStops = TripFinder.getAllStopTimesFromTrip(tripMeta.tripId);
        if (allTheoreticalStops.isEmpty())
            return null;

        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(tripMeta.tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        int resolvedDirection = resolveDirectionId(tripMeta, directionId, tripMeta.tripId);
        String routeId = tripMeta.routeId != null ? tripMeta.routeId : lineId;

        GtfsRealtime.TripDescriptor.Builder tripDescriptor = tripUpdate.getTripBuilder()
                .setTripId(tripMeta.tripId)
                .setRouteId(routeId)
                .setDirectionId(resolvedDirection)
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED);
        if (tripMeta.startDate != null && !tripMeta.startDate.isEmpty()) {
            tripDescriptor.setStartDate(tripMeta.startDate);
        }

        int seq = 1;
        for (String row : allTheoreticalStops) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4)
                continue;
            String stopId = parts[0];
            long theoreticalArr = parseSec(parts[1], serviceDayStartEpoch);
            long theoreticalDep = parseSec(parts[2], serviceDayStartEpoch);

            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stu = tripUpdate.addStopTimeUpdateBuilder();
            stu.setStopSequence(seq++);
            stu.setStopId(stopId);

            if (theoreticalArr != Long.MIN_VALUE) {
                stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                        .setTime(theoreticalArr + medianDelay).build());
            }
            if (theoreticalDep != Long.MIN_VALUE) {
                stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                        .setTime(theoreticalDep + medianDelay).build());
            }
        }

        if (tripUpdate.getStopTimeUpdateCount() == 0)
            return null;
        return entityBuilder.build();
    }

    /**
     * Cleans up trip states that are stale or whose last stop has already passed,
     * and removes expired entries from the blacklisted trip match cache.
     */
    private void cleanupStaleTripStates() {
        long currentTime = Instant.now().atZone(ZONE_ID).toLocalDateTime().atZone(ZONE_ID).toEpochSecond();

        // Normal trips: expire if last stop has passed OR no update for 15 min
        tripStates.entrySet().removeIf(entry -> {
            TripState s = entry.getValue();
            boolean lastStopPassed = s.lastStopEpoch > 0 && s.lastStopEpoch < currentTime;
            boolean stale = currentTime - s.lastUpdate > 15 * 60;
            return lastStopPassed || stale;
        });
        vehicleToTrip.entrySet().removeIf(e -> !tripStates.containsKey(e.getValue()));

        // Blacklisted trips: expire if last stop has passed OR no successful update for
        // 10 min
        blacklistedMatchCache.entrySet().removeIf(entry -> {
            BlacklistedTripCache c = entry.getValue();
            boolean lastStopPassed = c.lastStopEpoch > 0 && c.lastStopEpoch < currentTime;
            boolean stale = currentTime - c.lastUpdated > 10 * 60;
            return lastStopPassed || stale;
        });
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
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new java.io.File("entities_trips.json"),
                    entitiesTripsJson);
            System.out.println("Entities trips written to entities_trips.json");
        } catch (Exception e) {
            logger.error("Error writing entities trips to JSON: {}", e.getMessage(), e);
        }
    }


    /**
     * Processes a single EstimatedVehicleJourney entity from SIRI Lite data.
     * 
     * <p>
     * This method performs the core trip matching and entity building:
     * <ol>
     * <li>Extracts line, vehicle, destination, and direction information</li>
     * <li>Checks if vehicle already has a cached trip assignment</li>
     * <li>If not cached, builds a list of estimated calls and matches to a
     * theoretical GTFS trip</li>
     * <li>Updates trip state and vehicle associations</li>
     * <li>Builds a GTFS-RT TripUpdate entity with stop time predictions</li>
     * <li>Handles canceled trips and skipped stops</li>
     * </ol>
     * 
     * @param entity        the SIRI Lite EstimatedVehicleJourney JSON node
     * @param index         the original position in the input list (for ordering)
     * @param entitiesTrips optional map for debug output
     * @param context       processing context for tracking statistics
     * @return an IndexedEntity containing the built GTFS-RT entity, or null if
     *         processing fails
     */
    private IndexedEntity processEntity(JsonNode entity, int index, Map<String, JsonNode> entitiesTrips) {
        // Blacklisted operators are handled separately by processAllBlacklistedEntities
        if (isOperatorBlacklisted(entity)) {
            return null;
        }

        String lineId = "IDFM:" + entity.get("LineRef").get(FIELD_VALUE).asText().split(":")[3];
        String vehicleId = entity.get("DatedVehicleJourneyRef").get(FIELD_VALUE).asText();

        DirectionInfo directionInfo = extractDirectionInfo(entity, vehicleId);

        String destinationId = extractDestinationId(entity);
        if (destinationId == null)
            return null;

        List<JsonNode> estimatedCalls = getSortedEstimatedCalls(entity);
        List<org.jouca.idfm_gtfs_rt.records.EstimatedCall> estimatedCallList = buildEstimatedCallList(estimatedCalls);

        if (lineId.isEmpty() || estimatedCallList.isEmpty()) {
            return null;
        }

        // Check if this vehicle already has a cached trip assignment
        String tripId = vehicleToTrip.get(vehicleId);

        if (tripId != null) {
            // Verify the cached trip is still valid (exists in GTFS, matches the line, and
            // is temporally close)
            if (!isCachedTripValid(tripId, lineId, estimatedCalls)) {
                vehicleToTrip.remove(vehicleId);
                tripStates.remove(tripId);
                tripId = null;
            }
        }

        // If no cached trip or cache was invalid, perform trip matching
        if (tripId == null) {
            // First try direct vehicle-ref lookup (works for SNCF/Transilien whose UUID
            // appears in trip_id)
            tripId = TripFinder.findTripIdByVehicleRef(vehicleId, lineId);
            if (tripId == null) {
                tripId = findTripId(lineId, estimatedCallList, estimatedCalls, destinationId, directionInfo);
            }
            if (tripId == null || tripId.isEmpty()) {
                // No GTFS trip could be matched to this real, live-tracked vehicle — emit it as
                // ADDED rather than silently dropping it. IDFM's own ExtraJourney flag on SIRI
                // entities is not a reliable signal to gate this on: in practice it is never set
                // to true even for vehicles that genuinely have no GTFS counterpart, so relying
                // on it meant real vehicles vanished from the feed with no trace whenever
                // matching failed.
                GtfsRealtime.FeedEntity extraEntity = buildExtraJourneyFeedEntity(
                        vehicleId, lineId, directionInfo.directionIdForMatching(), estimatedCalls);
                if (extraEntity == null)
                    return null;
                return new IndexedEntity(index, extraEntity);
            }
        }

        // Determine service date based on the trip's theoretical GTFS schedule
        String serviceDate = determineServiceDateFromTrip(tripId);
        TripFinder.TripMeta tripMeta = TripFinder.getTripMeta(tripId, serviceDate);

        TripState state = updateTripState(tripId, vehicleId, tripMeta);

        GtfsRealtime.FeedEntity feedEntity = buildFeedEntity(tripId, state, tripMeta,
                directionInfo.directionIdForMatching(),
                lineId, estimatedCalls, destinationId);

        if (entitiesTrips != null) {
            entitiesTrips.put(tripId, entity);
        }
        return new IndexedEntity(index, feedEntity);
    }

    /**
     * Validates that a cached trip ID is still valid.
     * Checks if the trip exists in the trip states, belongs to the expected line,
     * and is temporally close to the current real-time data.
     * 
     * @param tripId         the cached trip ID to validate
     * @param expectedLineId the expected line ID
     * @param estimatedCalls the current estimated calls from SIRI
     * @return true if the cached trip is valid, false otherwise
     */
    private boolean isCachedTripValid(String tripId, String expectedLineId, List<JsonNode> estimatedCalls) {
        if (tripId == null || expectedLineId == null || estimatedCalls == null || estimatedCalls.isEmpty()) {
            return false;
        }

        // Check if trip exists in our state
        TripState state = tripStates.get(tripId);
        if (state == null) {
            return false;
        }

        // Verify the trip belongs to the expected line by getting trip metadata
        String serviceDate = determineServiceDateFromTrip(tripId);
        TripFinder.TripMeta tripMeta = TripFinder.getTripMeta(tripId, serviceDate);
        if (tripMeta == null || tripMeta.routeId == null) {
            return false;
        }

        // Check if the route matches the expected line
        if (!tripMeta.routeId.equals(expectedLineId)) {
            return false;
        }

        // Verify temporal consistency: the cached trip should still be close in time to
        // current SIRI data
        // Get the first stop time from SIRI
        JsonNode firstCall = estimatedCalls.get(0);
        String siriTime = extractTimeFromCall(firstCall);
        if (siriTime == null) {
            return false;
        }

        long siriEpochSeconds = Instant.parse(siriTime).atZone(ZONE_ID).toEpochSecond();

        // Get the first stop time from the theoretical trip
        Integer firstStopTimeSeconds = TripFinder.getFirstStopTime(tripId);
        if (firstStopTimeSeconds == null) {
            return false;
        }

        // Convert theoretical time to epoch seconds for today's service date
        LocalDate today = LocalDate.now(ZONE_ID);
        if (firstStopTimeSeconds >= 86400) {
            // Trip crosses midnight, use yesterday as service date
            today = today.minusDays(1);
        }

        long theoreticalEpochSeconds = today.atStartOfDay(ZONE_ID).toEpochSecond() + firstStopTimeSeconds;

        // Allow up to 10 minutes difference (600 seconds) for cached trips
        // This is stricter than the initial matching which allows up to 60 minutes
        long timeDifference = Math.abs(siriEpochSeconds - theoreticalEpochSeconds);
        return timeDifference <= 600;
    }

    /**
     * Checks if the operator of an entity is in the blacklist.
     * Supports wildcard patterns with '*' in the blacklist entries.
     * For example, "RATP-SIV:*" will match any operator starting with "RATP-SIV:"
     * 
     * @param entity the SIRI Lite entity to check
     * @return true if the operator is blacklisted, false otherwise
     */
    private boolean isOperatorBlacklisted(JsonNode entity) {
        if (!entity.has(FIELD_OPERATOR_REF)) {
            return false;
        }

        JsonNode operatorRef = entity.get(FIELD_OPERATOR_REF);
        if (!operatorRef.has(FIELD_VALUE)) {
            return false;
        }

        String operatorId = operatorRef.get(FIELD_VALUE).asText();

        // Check for exact matches or wildcard patterns
        for (String blacklistedPattern : OPERATOR_BLACKLIST) {
            if (matchesPattern(operatorId, blacklistedPattern)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if an operator ID matches a blacklist pattern.
     * Supports wildcard '*' which matches any sequence of characters.
     * 
     * @param operatorId the operator ID to check
     * @param pattern    the blacklist pattern (may contain '*' wildcards)
     * @return true if the operator ID matches the pattern
     */
    private boolean matchesPattern(String operatorId, String pattern) {
        if (operatorId == null || pattern == null) {
            return false;
        }

        // If no wildcard, do exact match
        if (!pattern.contains("*")) {
            return operatorId.equals(pattern);
        }

        // Convert wildcard pattern to regex
        // Escape special regex characters except *
        String regexPattern = pattern
                .replace("\\", "\\\\")
                .replace(".", "\\.")
                .replace("+", "\\+")
                .replace("?", "\\?")
                .replace("(", "\\(")
                .replace(")", "\\)")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("{", "\\{")
                .replace("}", "\\}")
                .replace("^", "\\^")
                .replace("$", "\\$")
                .replace("|", "\\|")
                .replace("*", ".*"); // Replace * with .* (any characters)

        return operatorId.matches(regexPattern);
    }

    /**
     * Record to hold direction and journey note information extracted from an
     * entity.
     * fallbackDirectionId is always derived from the entity and used when
     * journey-note matching fails.
     */
    private record DirectionInfo(Integer directionIdForMatching, String journeyNote, boolean journeyNoteDetailled,
            Integer fallbackDirectionId) {
    }

    /**
     * Determines the service date based on the trip's theoretical GTFS schedule.
     * 
     * <p>
     * For trips that cross midnight (with GTFS stop times >= 24:00:00), the service
     * date
     * must be the date when the trip started (the previous calendar day), not the
     * current date.
     * 
     * <p>
     * This method retrieves the first stop_time from the GTFS database for the
     * given trip.
     * If the time is in the range [24:00:00, 32:00:00] (represented as seconds >=
     * 86400),
     * the service date is set to one day before the current date.
     * 
     * @param tripId the GTFS trip identifier
     * @return service date in YYYYMMDD format
     */
    private String determineServiceDateFromTrip(String tripId) {
        LocalDate currentDate = LocalDate.now(ZONE_ID);

        if (tripId == null || tripId.isEmpty()) {
            return currentDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }

        // Get the first stop time for this trip from GTFS
        Integer firstStopTimeSeconds = TripFinder.getFirstStopTime(tripId);

        if (firstStopTimeSeconds == null) {
            // If we can't determine the first stop time, use current date
            return currentDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }

        // If the first stop time is >= 24:00:00 (86400 seconds), the trip crosses
        // midnight.
        // Whether the service day is today or yesterday depends on the current
        // wall-clock time:
        // - After midnight (hour < 8): the trip's physical departure is today but
        // belongs to
        // yesterday's service day → subtract 1 day.
        // - Before midnight (hour >= 8): the trip runs tonight into tomorrow; the
        // service day
        // is still today → use current date.
        if (firstStopTimeSeconds >= 86400 && firstStopTimeSeconds < 115200) {
            ZonedDateTime nowZdt = ZonedDateTime.now(ZONE_ID);
            if (nowZdt.getHour() < 8) {
                return currentDate.minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            }
            return currentDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }

        // Otherwise, use the current date as the service date
        return currentDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    /**
     * Extracts direction and journey note information from SIRI Lite entity.
     * 
     * @param entity    the SIRI Lite entity
     * @param vehicleId the vehicle identifier
     * @return DirectionInfo containing direction ID, journey note, and detail flag
     */
    private DirectionInfo extractDirectionInfo(JsonNode entity, String vehicleId) {
        Integer directionIdForMatching = null;
        String journeyNote = null;
        boolean journeyNoteDetailled = false;

        // Always derive direction so it can be used as fallback when journey-note
        // matching fails
        int direction = determineDirection(entity);
        Integer fallbackDirectionId = (direction != -1) ? direction : null;

        if (entity.get(FIELD_JOURNEY_NOTE) != null &&
                entity.get(FIELD_JOURNEY_NOTE).size() > 0 &&
                entity.get(FIELD_JOURNEY_NOTE).get(0).get(FIELD_VALUE) != null &&
                entity.get(FIELD_JOURNEY_NOTE).get(0).get(FIELD_VALUE).asText().matches("^[A-Z]{4}$")) {
            journeyNote = entity.get(FIELD_JOURNEY_NOTE).get(0).get(FIELD_VALUE).asText();
        } else {
            directionIdForMatching = fallbackDirectionId;
        }

        if (vehicleId.matches("(?<=RATP\\.)[A-Z0-9]+(?=:|$)")) {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(?<=RATP\\.)[A-Z0-9]+(?=:)")
                    .matcher(vehicleId);
            if (matcher.find()) {
                journeyNote = matcher.group();
            } else {
                journeyNote = vehicleId;
            }
            journeyNoteDetailled = true;
        }

        return new DirectionInfo(directionIdForMatching, journeyNote, journeyNoteDetailled, fallbackDirectionId);
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
            if (stopId == null)
                continue;

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
     * @param lineId            the line identifier
     * @param estimatedCallList the list of estimated calls
     * @param estimatedCalls    the original JSON nodes
     * @param destinationId     the destination stop ID
     * @param directionInfo     the direction information
     * @return the matched trip ID, or null if not found
     */
    private String findTripId(String lineId, List<org.jouca.idfm_gtfs_rt.records.EstimatedCall> estimatedCallList,
            List<JsonNode> estimatedCalls, String destinationId, DirectionInfo directionInfo) {
        boolean isArrivalTime = !estimatedCallList.isEmpty() &&
                (estimatedCalls.get(0).has(FIELD_EXPECTED_ARRIVAL_TIME) ||
                        estimatedCalls.get(0).has(FIELD_AIMED_ARRIVAL_TIME));

        try {
            String result = TripFinder.findTripIdFromEstimatedCalls(lineId, estimatedCallList, isArrivalTime,
                    destinationId, directionInfo.journeyNote(), directionInfo.journeyNoteDetailled(),
                    directionInfo.directionIdForMatching());

            // If journey-note matching failed (non-RATP notes only), retry using direction
            // instead
            if (result == null && directionInfo.journeyNote() != null && !directionInfo.journeyNoteDetailled()) {
                result = TripFinder.findTripIdFromEstimatedCalls(lineId, estimatedCallList, isArrivalTime,
                        destinationId, null, false, directionInfo.fallbackDirectionId());
            }

            return result;
        } catch (SQLException e) {
            logger.debug("Error finding trip ID for lineId: {}, destinationId: {}", lineId, destinationId, e);
            return null;
        }
    }

    /**
     * Updates trip state and vehicle associations.
     * Lazily computes {@code lastStopEpoch} on first encounter using trip metadata.
     *
     * @param tripId    the trip identifier
     * @param vehicleId the vehicle identifier
     * @param tripMeta  the trip metadata (used to compute last stop epoch)
     * @return the updated trip state
     */
    private TripState updateTripState(String tripId, String vehicleId, TripFinder.TripMeta tripMeta) {
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

        // Lazily compute the last stop epoch so we can expire the state precisely
        if (state.lastStopEpoch == 0 && tripMeta != null) {
            Integer lastSec = TripFinder.getLastStopTime(tripId);
            if (lastSec != null) {
                java.time.LocalDate svcDay;
                try {
                    svcDay = (tripMeta.startDate != null && !tripMeta.startDate.isEmpty())
                            ? java.time.LocalDate.parse(tripMeta.startDate,
                                    java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))
                            : java.time.LocalDate.now(ZONE_ID);
                } catch (Exception e) {
                    svcDay = java.time.LocalDate.now(ZONE_ID);
                }
                state.lastStopEpoch = svcDay.atStartOfDay(ZONE_ID).toEpochSecond() + lastSec;
            }
        }

        // Store route/direction for cache-only re-emission
        if (tripMeta != null) {
            if (state.routeId == null)
                state.routeId = tripMeta.routeId;
            state.directionId = tripMeta.directionId;
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
     * @param tripId                 the trip identifier
     * @param state                  the trip state
     * @param tripMeta               the trip metadata
     * @param directionIdForMatching the direction ID from SIRI Lite
     * @param lineId                 the line identifier
     * @param estimatedCalls         the list of estimated calls
     * @return the built FeedEntity
     */
    private GtfsRealtime.FeedEntity buildFeedEntity(String tripId, TripState state, TripFinder.TripMeta tripMeta,
            Integer directionIdForMatching, String lineId,
            List<JsonNode> estimatedCalls, String destinationId) {
        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        int directionId = resolveDirectionId(tripMeta, directionIdForMatching, tripId);
        String routeForDescriptor = tripMeta != null && tripMeta.routeId != null ? tripMeta.routeId : lineId;

        GtfsRealtime.TripDescriptor.Builder tripDescriptor = tripUpdate.getTripBuilder()
                .setRouteId(routeForDescriptor)
                .setDirectionId(directionId)
                .setTripId(tripId);

        if (tripMeta != null && tripMeta.startDate != null && !tripMeta.startDate.isEmpty()) {
            tripDescriptor.setStartDate(tripMeta.startDate);
        }

        tripUpdate.getVehicleBuilder().setId(state.vehicleId);

        addStopTimeUpdates(tripUpdate, estimatedCalls, tripId, destinationId, state);

        // Detect partial trip: destinationId set and not the theoretical terminus
        if (destinationId != null) {
            List<String> allTheoreticalStops = TripFinder.getAllStopTimesFromTrip(tripId);
            if (!allTheoreticalStops.isEmpty()) {
                String theoreticalTerminus = allTheoreticalStops.get(allTheoreticalStops.size() - 1).split(",", 4)[0];
                if (!destinationId.equals(theoreticalTerminus)) {
                    List<String> emittedStops = new ArrayList<>();
                    for (String row : allTheoreticalStops) {
                        String sid = row.split(",", 4)[0];
                        emittedStops.add(sid);
                        if (sid.equals(destinationId))
                            break;
                    }
                    String destName = estimatedCalls.isEmpty() ? destinationId
                            : estimatedCalls.get(0).path("DestinationDisplay").path(0).path(FIELD_VALUE)
                                    .asText(destinationId);
                    partialTrips.put(tripId, new PartialTripInfo(
                            tripId, lineId, lineId.startsWith("IDFM:") ? lineId.substring(5) : lineId,
                            directionId, theoreticalTerminus, destinationId, destName,
                            emittedStops, Instant.now().getEpochSecond()));
                }
            }
        }

        return entityBuilder.build();
    }

    /**
     * Resolves the direction ID from available sources.
     * 
     * @param tripMeta               the trip metadata
     * @param directionIdForMatching the direction from SIRI Lite
     * @param tripId                 the trip identifier
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
     * @param tripUpdate     the trip update builder
     * @param estimatedCalls the list of estimated calls
     * @param tripId         the trip identifier
     */
    private void addStopTimeUpdates(GtfsRealtime.TripUpdate.Builder tripUpdate, List<JsonNode> estimatedCalls,
            String tripId, String destinationId, TripState state) {
        boolean allCancelled = estimatedCalls.stream().allMatch(call -> (call.has(FIELD_DEPARTURE_STATUS)
                && (call.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_CANCELLED)
                        || call.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_MISSED)))
                ||
                (call.has(FIELD_ARRIVAL_STATUS) && (call.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_CANCELLED)
                        || call.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_MISSED))));

        if (allCancelled) {
            tripUpdate.getTripBuilder()
                    .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        } else {
            // Compute a constant anchor delay from the first call that has both Expected
            // and Aimed times.
            // This prevents growing delays when SIRI future-stop predictions accumulate
            // additional offset.
            long anchorDelay = Long.MIN_VALUE;
            for (JsonNode call : estimatedCalls) {
                if (call.has(FIELD_EXPECTED_ARRIVAL_TIME) && call.has(FIELD_AIMED_ARRIVAL_TIME)) {
                    long expected = parseTime(call.get(FIELD_EXPECTED_ARRIVAL_TIME).asText());
                    long aimed = parseTime(call.get(FIELD_AIMED_ARRIVAL_TIME).asText());
                    if (expected != Long.MIN_VALUE && aimed != Long.MIN_VALUE) {
                        anchorDelay = expected - aimed;
                        break;
                    }
                } else if (call.has(FIELD_EXPECTED_DEPARTURE_TIME) && call.has(FIELD_AIMED_DEPARTURE_TIME)) {
                    long expected = parseTime(call.get(FIELD_EXPECTED_DEPARTURE_TIME).asText());
                    long aimed = parseTime(call.get(FIELD_AIMED_DEPARTURE_TIME).asText());
                    if (expected != Long.MIN_VALUE && aimed != Long.MIN_VALUE) {
                        anchorDelay = expected - aimed;
                        break;
                    }
                }
            }

            // Persist anchor delay into state for cache-only re-emission cycles
            if (anchorDelay != Long.MIN_VALUE && state != null) {
                state.lastKnownDelaySeconds = anchorDelay;
            }

            List<String> stopTimeUpdates = new ArrayList<>();
            for (JsonNode estimatedCall : estimatedCalls) {
                processEstimatedCall(estimatedCall, tripUpdate, tripId, stopTimeUpdates, anchorDelay);
                // For partial trips: stop processing after the destination stop
                if (destinationId != null && estimatedCall.has(FIELD_STOP_POINT_REF)) {
                    String stopCode = estimatedCall.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3];
                    String stopId = TripFinder.resolveStopId(stopCode);
                    if (destinationId.equals(stopId))
                        break;
                }
            }
            // Clear invalid stop times where time goes backward
            clearInvalidStopTimes(tripUpdate);
        }
    }

    /**
     * Determines the direction ID from SIRI Lite entity data.
     * 
     * <p>
     * Attempts to extract direction from:
     * <ul>
     * <li>DirectionRef field (parsing IDFM format codes)</li>
     * <li>DirectionName field (French and English labels)</li>
     * </ul>
     * 
     * <p>
     * Direction mapping:
     * <ul>
     * <li>0: Outbound/Retour (R)</li>
     * <li>1: Inbound/Aller (A)</li>
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
     * <p>
     * IDFM stop IDs should be numeric after the colon separators.
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
     * <p>
     * Uses a cache to avoid repeated parsing of the same timestamps,
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
     * <p>
     * Estimated calls are sorted by time to ensure they appear in chronological
     * order, which is required for GTFS-RT stop time updates. The method checks
     * multiple time fields in order of preference:
     * <ol>
     * <li>ExpectedArrivalTime</li>
     * <li>ExpectedDepartureTime</li>
     * <li>AimedArrivalTime</li>
     * <li>AimedDepartureTime</li>
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
     * <p>
     * Checks time fields in order of preference and returns the epoch seconds
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
     * <p>
     * This method:
     * <ul>
     * <li>Filters out past stop times (before current time)</li>
     * <li>Resolves stop IDs and finds the correct stop sequence</li>
     * <li>Extracts arrival and departure predictions</li>
     * <li>Handles skipped stops (marked as CANCELLED in SIRI Lite)</li>
     * </ul>
     * 
     * <p>
     * <strong>Note:</strong> Stop time updates are added in the order processed.
     * Sorting is performed once at the end of processing all EstimatedCalls to
     * ensure
     * chronological order in the GTFS-RT feed.
     * 
     * @param estimatedCall   the SIRI Lite EstimatedCall JSON node
     * @param tripUpdate      the GTFS-RT TripUpdate builder to add stop time update
     * @param tripId          the GTFS trip ID for stop sequence lookup
     * @param stopTimeUpdates list tracking which stop sequences have been processed
     */
    private void processEstimatedCall(JsonNode estimatedCall, GtfsRealtime.TripUpdate.Builder tripUpdate, String tripId,
            List<String> stopTimeUpdates, long anchorDelay) {
        // Emit SKIPPED for MISSED/CANCELLED stops even if their times are in the past
        if (hasSkippedStatus(estimatedCall)) {
            String stopId = TripFinder
                    .resolveStopId(estimatedCall.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3]);
            if (stopId == null)
                return;
            String stopSequence = TripFinder.findStopSequence(tripId, stopId, stopTimeUpdates);
            if (stopSequence == null)
                return;
            stopTimeUpdates.add(stopSequence);
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = tripUpdate.addStopTimeUpdateBuilder();
            stopTimeUpdate.setStopSequence(Integer.parseInt(stopSequence));
            stopTimeUpdate.setStopId(stopId);
            stopTimeUpdate.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
            return;
        }

        // Skip if times are in the past
        if (isEstimatedCallInPast(estimatedCall)) {
            return;
        }

        String stopId = TripFinder
                .resolveStopId(estimatedCall.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3]);
        if (stopId == null)
            return;

        String stopSequence = TripFinder.findStopSequence(tripId, stopId, stopTimeUpdates);
        if (stopSequence == null)
            return;

        stopTimeUpdates.add(stopSequence);

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = tripUpdate.addStopTimeUpdateBuilder();
        stopTimeUpdate.setStopSequence(Integer.parseInt(stopSequence));
        stopTimeUpdate.setStopId(stopId);

        // Set arrival and departure times
        setArrivalTime(estimatedCall, stopTimeUpdate, anchorDelay);
        setDepartureTime(estimatedCall, stopTimeUpdate, anchorDelay);

        // Handle cancellations
        handleCancellationStatus(estimatedCall, stopTimeUpdate);
    }

    /**
     * Checks if the estimated call is in the past and should be skipped.
     * 
     * @param estimatedCall the estimated call to check
     * @return true if the call is in the past, false otherwise
     */
    private boolean hasSkippedStatus(JsonNode estimatedCall) {
        boolean departureMissedOrCancelled = estimatedCall.has(FIELD_DEPARTURE_STATUS) &&
                (estimatedCall.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_CANCELLED) ||
                        estimatedCall.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_MISSED));
        boolean arrivalMissedOrCancelled = estimatedCall.has(FIELD_ARRIVAL_STATUS) &&
                (estimatedCall.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_CANCELLED) ||
                        estimatedCall.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_MISSED));
        return departureMissedOrCancelled || arrivalMissedOrCancelled;
    }

    private boolean isEstimatedCallInPast(JsonNode estimatedCall) {
        // Check arrival times
        if (estimatedCall.has(FIELD_EXPECTED_ARRIVAL_TIME)) {
            long arrivalTime = parseTime(estimatedCall.get(FIELD_EXPECTED_ARRIVAL_TIME).asText());
            if (arrivalTime < currentEpochSecond)
                return true;
        } else if (estimatedCall.has(FIELD_AIMED_ARRIVAL_TIME)) {
            long arrivalTime = parseTime(estimatedCall.get(FIELD_AIMED_ARRIVAL_TIME).asText());
            if (arrivalTime < currentEpochSecond)
                return true;
        }

        // Check departure times
        if (estimatedCall.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_EXPECTED_DEPARTURE_TIME).asText());
            if (departureTime < currentEpochSecond)
                return true;
        } else if (estimatedCall.has(FIELD_AIMED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_AIMED_DEPARTURE_TIME).asText());
            if (departureTime < currentEpochSecond)
                return true;
        }

        return false;
    }

    /**
     * Sets the arrival time on the stop time update from the estimated call.
     * Prefers ExpectedArrivalTime over AimedArrivalTime.
     * 
     * @param estimatedCall  the estimated call containing time data
     * @param stopTimeUpdate the stop time update builder to set the arrival time on
     */
    private void setArrivalTime(JsonNode estimatedCall, GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate,
            long anchorDelay) {
        if (anchorDelay != Long.MIN_VALUE && estimatedCall.has(FIELD_AIMED_ARRIVAL_TIME)) {
            long aimed = parseTime(estimatedCall.get(FIELD_AIMED_ARRIVAL_TIME).asText());
            if (aimed != Long.MIN_VALUE) {
                stopTimeUpdate.setArrival(
                        GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(aimed + anchorDelay).build());
                return;
            }
        }
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
     * @param estimatedCall  the estimated call containing time data
     * @param stopTimeUpdate the stop time update builder to set the departure time
     *                       on
     */
    private void setDepartureTime(JsonNode estimatedCall, GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate,
            long anchorDelay) {
        if (anchorDelay != Long.MIN_VALUE && estimatedCall.has(FIELD_AIMED_DEPARTURE_TIME)) {
            long aimed = parseTime(estimatedCall.get(FIELD_AIMED_DEPARTURE_TIME).asText());
            if (aimed != Long.MIN_VALUE) {
                stopTimeUpdate.setDeparture(
                        GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(aimed + anchorDelay).build());
                return;
            }
        }
        if (estimatedCall.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_EXPECTED_DEPARTURE_TIME).asText());
            stopTimeUpdate
                    .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
        } else if (estimatedCall.has(FIELD_AIMED_DEPARTURE_TIME)) {
            long departureTime = parseTime(estimatedCall.get(FIELD_AIMED_DEPARTURE_TIME).asText());
            stopTimeUpdate
                    .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departureTime).build());
        }
    }

    /**
     * Handles cancellation status for the stop time update.
     * If either departure or arrival is cancelled, marks the stop as SKIPPED
     * and clears the timing information.
     * 
     * @param estimatedCall  the estimated call containing status information
     * @param stopTimeUpdate the stop time update builder to update
     */
    private void handleCancellationStatus(JsonNode estimatedCall,
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate) {
        boolean isDepartureCancelled = estimatedCall.has(FIELD_DEPARTURE_STATUS) &&
                (estimatedCall.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_CANCELLED) ||
                        estimatedCall.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_MISSED));
        boolean isArrivalCancelled = estimatedCall.has(FIELD_ARRIVAL_STATUS) &&
                (estimatedCall.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_CANCELLED) ||
                        estimatedCall.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_MISSED));

        if (isDepartureCancelled || isArrivalCancelled) {
            stopTimeUpdate.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
            stopTimeUpdate.clearArrival();
            stopTimeUpdate.clearDeparture();
        }
    }

    /**
     * Collects results from completed futures and updates the progress bar.
     * 
     * <p>
     * This method iterates through the list of futures, retrieves their results,
     * and filters out null or empty entities. It also handles exceptions that may
     * occur during result retrieval and updates the progress bar after each entity
     * is processed.
     * 
     * @param futures the list of futures containing indexed entities
     * @param total   the total number of entities being processed (for progress
     *                bar)
     * @return a list of successfully processed indexed entities
     * @throws InterruptedException if the thread is interrupted while waiting for
     *                              results
     * @throws ExecutionException   if an entity processing task threw an exception
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
     * <p>
     * This method performs a graceful shutdown of the executor service:
     * <ol>
     * <li>Initiates an orderly shutdown</li>
     * <li>Waits up to 2 minutes for tasks to complete</li>
     * <li>Forces shutdown if tasks don't complete in time</li>
     * <li>Waits an additional 1 minute for forced shutdown</li>
     * <li>Handles interruptions by forcing shutdown and restoring interrupt
     * status</li>
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
     * <p>
     * Displays a text-based progress bar with percentage and count.
     * The progress bar is updated in-place using carriage return.
     * 
     * @param current the number of entities processed so far
     * @param total   the total number of entities to process
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
     * Clears invalid stop times in the trip update where the arrival or departure
     * time
     * is earlier than the previous stop's time, which indicates a time progression
     * error.
     * Also removes stop time updates where the stop sequence is out of order
     * (decreasing),
     * which indicates erroneous SIRI data from other vehicles.
     * 
     * <p>
     * This method:
     * <ul>
     * <li>Iterates through all stop time updates in chronological order</li>
     * <li>Tracks the maximum departure or arrival time seen so far</li>
     * <li>Tracks the maximum stop sequence seen so far</li>
     * <li>For each stop, compares its times AND sequence against the previous
     * maximums</li>
     * <li>If either arrival or departure time is earlier than the previous maximum,
     * OR if the stop sequence is less than the previous maximum (out of order),
     * the stop time update is removed to maintain temporal and sequential
     * consistency</li>
     * <li>Updates the maximum time and sequence trackers if the current stop's
     * values are valid</li>
     * </ul>
     * 
     * <p>
     * <strong>Use case:</strong> Handles cases where real-time SIRI data contains:
     * <ul>
     * <li>Time errors where a later stop shows an earlier time than the previous
     * stop</li>
     * <li>Sequence errors where SIRI mistakenly includes stop data from other
     * vehicles with incorrect stop sequences</li>
     * </ul>
     * This can occur in unreliable data sources like SIRI Lite where timestamps or
     * stop associations may be
     * incorrectly calculated or transmitted.
     * 
     * @param tripUpdate the trip update builder containing stop time updates to
     *                   validate
     */
    private void clearInvalidStopTimes(GtfsRealtime.TripUpdate.Builder tripUpdate) {
        long maxTime = Long.MIN_VALUE;
        int maxSequence = Integer.MIN_VALUE;
        List<Integer> indicesToRemove = new ArrayList<>();

        // Iterate through stop time updates and identify invalid ones
        for (int i = 0; i < tripUpdate.getStopTimeUpdateCount(); i++) {
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = tripUpdate.getStopTimeUpdateBuilder(i);
            long currentMinTime = Long.MAX_VALUE;
            boolean isInvalid = false;

            // Check if stop sequence is decreasing (out of order)
            if (stopTimeUpdate.hasStopSequence()) {
                int currentSequence = stopTimeUpdate.getStopSequence();
                if (currentSequence < maxSequence) {
                    isInvalid = true;
                } else {
                    maxSequence = currentSequence;
                }
            }

            // Check arrival time
            if (stopTimeUpdate.hasArrival()) {
                long arrivalTime = stopTimeUpdate.getArrival().getTime();
                if (arrivalTime < maxTime) {
                    isInvalid = true;
                } else {
                    currentMinTime = Math.min(currentMinTime, arrivalTime);
                }
            }

            // Check departure time
            if (stopTimeUpdate.hasDeparture()) {
                long departureTime = stopTimeUpdate.getDeparture().getTime();
                if (departureTime < maxTime) {
                    isInvalid = true;
                } else {
                    currentMinTime = Math.min(currentMinTime, departureTime);
                }
            }

            // Mark for removal if invalid
            if (isInvalid) {
                indicesToRemove.add(i);
            }

            // Update max time if we have valid times
            if (currentMinTime != Long.MAX_VALUE) {
                maxTime = currentMinTime;
            }
        }

        // Remove invalid stop time updates in reverse order to maintain indices
        for (int i = indicesToRemove.size() - 1; i >= 0; i--) {
            tripUpdate.removeStopTimeUpdate(indicesToRemove.get(i));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────────────
    // Trajectory-based blacklisted trip matching helpers
    // ─────────────────────────────────────────────────────────────────────────────────────

    /**
     * Builds reference stop data from the longest active trip for a given
     * direction.
     * The reference trip provides the canonical stop ordering used to convert SIRI
     * stop codes
     * to integer position keys in the trajectory maps.
     */
    private ReferenceStopData buildReferenceStopData(List<TripFinder.TripMeta> activeTrips,
            Integer directionId) {
        String longestTripId = null;
        int maxStops = 0;
        for (TripFinder.TripMeta meta : activeTrips) {
            if (directionId != null && meta.directionId != directionId)
                continue;
            List<String> stops = TripFinder.getAllStopTimesFromTrip(meta.tripId);
            if (stops.size() > maxStops) {
                maxStops = stops.size();
                longestTripId = meta.tripId;
            }
        }
        if (longestTripId == null)
            return null;

        List<String> stopRows = TripFinder.getAllStopTimesFromTrip(longestTripId);
        Map<String, Integer> stopIdToSeq = new java.util.LinkedHashMap<>();
        Map<Integer, String> seqToStopId = new java.util.LinkedHashMap<>();
        Map<Integer, Long> seqToDepSecs = new java.util.LinkedHashMap<>();

        for (String row : stopRows) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4)
                continue;
            String stopId = parts[0];
            try {
                int seq = Integer.parseInt(parts[3].trim());
                String depStr = parts[2];
                String arrStr = parts[1];
                long rawSecs = (depStr != null && !depStr.isEmpty() && !depStr.equals("null"))
                        ? Long.parseLong(depStr)
                        : Long.parseLong(arrStr);
                stopIdToSeq.put(stopId, seq);
                seqToStopId.put(seq, stopId);
                seqToDepSecs.put(seq, rawSecs);
            } catch (NumberFormatException ignored) {
            }
        }

        // Build inter-stop travel time: seq_i → (dep_secs_i − dep_secs_{i−1}) / 60.0
        Map<Integer, Double> seqToInterStopMinutes = new java.util.LinkedHashMap<>();
        List<Map.Entry<Integer, Long>> orderedSeqs = new ArrayList<>(seqToDepSecs.entrySet());
        orderedSeqs.sort(Map.Entry.comparingByKey());
        for (int i = 1; i < orderedSeqs.size(); i++) {
            int seq = orderedSeqs.get(i).getKey();
            long diffSecs = orderedSeqs.get(i).getValue() - orderedSeqs.get(i - 1).getValue();
            if (diffSecs > 0)
                seqToInterStopMinutes.put(seq, diffSecs / 60.0);
        }

        Map<String, String> codeToStopId = TripFinder.buildSiriCodeToStopIdForTrip(longestTripId);
        return new ReferenceStopData(codeToStopId, stopIdToSeq, seqToStopId, seqToDepSecs, seqToInterStopMinutes);
    }

    /**
     * Flattens all EstimatedCalls from a group of blacklisted entities into a list
     * of
     * {@link CallData} records, one per resolvable call.
     *
     * <p>
     * Stop codes are resolved exclusively through the reference trip's code map
     * (not the global
     * stops table) to prevent wrong-quai false matches.
     *
     * @param entities      blacklisted EstimatedVehicleJourney nodes for one
     *                      line+direction
     * @param refData       reference-trip stop data for stop-code → sequence
     *                      resolution
     * @param midnightEpoch Unix epoch of service-day midnight (Europe/Paris)
     */
    private List<CallData> flattenToCallData(List<JsonNode> entities,
            ReferenceStopData refData,
            long midnightEpoch) {
        List<CallData> result = new ArrayList<>();
        for (JsonNode entity : entities) {
            JsonNode callsNode = entity.path("EstimatedCalls").path("EstimatedCall");
            if (callsNode.isMissingNode())
                continue;
            for (JsonNode call : callsNode) {
                if (!checkStopIntegrity(call))
                    continue;
                String stopCode = call.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3];
                String stopId = refData.codeToStopId().get(stopCode);
                if (stopId == null)
                    continue;
                Integer seq = refData.stopIdToSeq().get(stopId);
                if (seq == null)
                    continue;

                long arrEpoch = Long.MIN_VALUE, depEpoch = Long.MIN_VALUE;
                if (call.has(FIELD_EXPECTED_ARRIVAL_TIME))
                    arrEpoch = parseTime(call.get(FIELD_EXPECTED_ARRIVAL_TIME).asText());
                if (call.has(FIELD_EXPECTED_DEPARTURE_TIME))
                    depEpoch = parseTime(call.get(FIELD_EXPECTED_DEPARTURE_TIME).asText());
                if (arrEpoch == Long.MIN_VALUE && call.has(FIELD_AIMED_ARRIVAL_TIME))
                    arrEpoch = parseTime(call.get(FIELD_AIMED_ARRIVAL_TIME).asText());
                if (depEpoch == Long.MIN_VALUE && call.has(FIELD_AIMED_DEPARTURE_TIME))
                    depEpoch = parseTime(call.get(FIELD_AIMED_DEPARTURE_TIME).asText());

                long obsEpoch = depEpoch != Long.MIN_VALUE ? depEpoch : arrEpoch;
                if (obsEpoch == Long.MIN_VALUE)
                    continue;

                double absMinutes = (obsEpoch - midnightEpoch) / 60.0;
                result.add(new CallData(seq, stopId, arrEpoch, depEpoch, absMinutes));
            }
        }
        return result;
    }

    /**
     * Groups flattened real-time calls into distinct vehicle trajectories by
     * detecting
     * descents in the minimum-time envelope across stop sequences.
     *
     * <p>
     * Algorithm (mirrors the Python {@code group_points} function):
     * <ol>
     * <li>At the <em>first</em> stop sequence every observed call becomes its own
     * group
     * (each is a distinct vehicle at the route origin).</li>
     * <li>For each subsequent stop sequence, find the call with the minimum
     * absolute time.
     * Subtract half the theoretical inter-stop travel time ({@code offset/2}) to
     * compensate for vehicles running slightly faster than schedule.</li>
     * <li>If this adjusted minimum is <em>lower</em> than the previous minimum, a
     * new
     * earlier vehicle has appeared → start a new group.</li>
     * </ol>
     *
     * @param calls   flattened call data for one line+direction
     * @param offsets stop_sequence → theoretical travel time from previous stop
     *                (minutes)
     * @return ordered map of group index → list of representative calls for that
     *         vehicle
     */
    private Map<Integer, List<CallData>> groupPoints(List<CallData> calls,
            Map<Integer, Double> offsets) {
        // Bucket calls by stop_sequence (sorted)
        Map<Integer, List<CallData>> bySeq = new java.util.TreeMap<>();
        for (CallData call : calls) {
            bySeq.computeIfAbsent(call.stopSeq(), k -> new ArrayList<>()).add(call);
        }
        List<Integer> sequences = new ArrayList<>(bySeq.keySet());
        if (sequences.isEmpty())
            return java.util.Collections.emptyMap();

        Map<Integer, List<CallData>> result = new java.util.LinkedHashMap<>();
        int currentIndex = 0;

        // First sequence: each call is its own distinct vehicle group, sorted earliest
        // first
        int firstSeq = sequences.get(0);
        List<CallData> firstCalls = new ArrayList<>(bySeq.get(firstSeq));
        firstCalls.sort(Comparator.comparingDouble(CallData::absoluteMinutes));
        for (CallData call : firstCalls) {
            List<CallData> group = new ArrayList<>();
            group.add(call);
            result.put(currentIndex++, group);
        }

        Double prevMin = null;

        for (int seq : sequences) {
            CallData minCall = bySeq.get(seq).stream()
                    .min(Comparator.comparingDouble(CallData::absoluteMinutes))
                    .orElse(null);
            if (minCall == null)
                continue;

            double offset = offsets.getOrDefault(seq, 0.0);
            // Subtract half the inter-stop time so slightly-faster vehicles still appear
            // as the same vehicle rather than triggering a false new-group split.
            double currentMin = minCall.absoluteMinutes() - (offset / 2.0);

            if (prevMin == null) {
                prevMin = currentMin;
                continue;
            }

            if (currentMin < prevMin) {
                // Minimum is descending → a new, earlier vehicle has appeared
                List<CallData> group = new ArrayList<>();
                group.add(minCall);
                result.put(currentIndex++, group);
            }

            prevMin = currentMin;
        }

        return result;
    }

    /**
     * Matches a single vehicle trajectory to the theoretical trip with the lowest
     * Median Absolute
     * Deviation (MAD) of delays. Trips already claimed by another trajectory are
     * excluded.
     *
     * @param trajectory   position → observed epoch time for this vehicle
     * @param activeTrips  candidate theoretical trips for this line + direction
     * @param refData      reference stop data for position → stop_id conversion
     * @param madThreshold maximum acceptable MAD (seconds); higher = noisier data
     *                     tolerated
     * @param usedTripIds  set of trip IDs already matched; updated by caller after
     *                     a match
     * @return best match, or {@code null} if no trip meets the MAD threshold
     */
    private TrajectoryMatch matchTrajectoryToTrip(
            Map<Integer, Long> trajectory,
            List<TripFinder.TripMeta> activeTrips,
            ReferenceStopData refData,
            long madThreshold,
            Set<String> usedTripIds) {

        TrajectoryMatch best = null;
        long bestMad = Long.MAX_VALUE;
        int bestMatchCount = 0;

        // Pre-build: reference stop_id → set of SIRI codes (inverse of codeToStopId).
        // Used as a fallback when a candidate trip has a different stop_id for the same
        // physical stop (e.g., ref=IDFM:36131, candidate=IDFM:26537 both map to code
        // "26537").
        Map<String, java.util.Set<String>> refStopIdToSiriCodes = new java.util.HashMap<>();
        for (Map.Entry<String, String> entry : refData.codeToStopId().entrySet()) {
            refStopIdToSiriCodes
                    .computeIfAbsent(entry.getValue(), k -> new java.util.HashSet<>())
                    .add(entry.getKey());
        }

        for (TripFinder.TripMeta meta : activeTrips) {
            if (usedTripIds.contains(meta.tripId))
                continue;

            long svcEpoch;
            try {
                java.time.LocalDate svcDay = (meta.startDate != null && !meta.startDate.isEmpty())
                        ? java.time.LocalDate.parse(meta.startDate,
                                java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))
                        : java.time.LocalDate.now(ZONE_ID);
                svcEpoch = svcDay.atStartOfDay(ZONE_ID).toEpochSecond();
            } catch (Exception e) {
                svcEpoch = java.time.LocalDate.now(ZONE_ID).atStartOfDay(ZONE_ID).toEpochSecond();
            }

            List<String> stopRows = TripFinder.getAllStopTimesFromTrip(meta.tripId);
            if (stopRows.isEmpty())
                continue;

            // Build stop_id → dep_secs and bareSuffix → dep_secs for this trip.
            // The bare suffix fallback handles cases where the candidate trip uses a
            // stop_id
            // whose numeric suffix is a SIRI code of the reference trip's stop (e.g.
            // IDFM:26537
            // has suffix "26537" which is also a SIRI code of reference stop IDFM:36131).
            Map<String, Long> stopToDepSecs = new java.util.HashMap<>();
            Map<String, Long> bareSuffixToDepSecs = new java.util.HashMap<>();
            for (String row : stopRows) {
                String[] parts = row.split(",", 4);
                if (parts.length < 4)
                    continue;
                try {
                    String depStr = parts[2];
                    String arrStr = parts[1];
                    long rawSecs = (depStr != null && !depStr.isEmpty() && !depStr.equals("null"))
                            ? Long.parseLong(depStr)
                            : Long.parseLong(arrStr);
                    String stopId = parts[0];
                    stopToDepSecs.put(stopId, rawSecs);
                    String bare = stopId.contains(":") ? stopId.substring(stopId.lastIndexOf(':') + 1) : stopId;
                    bareSuffixToDepSecs.put(bare, rawSecs);
                } catch (NumberFormatException ignored) {
                }
            }

            // Compute delay at each trajectory position that overlaps with this trip.
            // Try direct stop_id match first; fall back to SIRI-code → bare-suffix lookup
            // to handle route variants with different but physically identical stops.
            List<Long> delays = new ArrayList<>();
            for (Map.Entry<Integer, Long> e : trajectory.entrySet()) {
                String stopId = refData.seqToStopId().get(e.getKey());
                if (stopId == null)
                    continue;
                Long depSecs = stopToDepSecs.get(stopId);
                if (depSecs == null) {
                    java.util.Set<String> codes = refStopIdToSiriCodes.get(stopId);
                    if (codes != null) {
                        for (String code : codes) {
                            depSecs = bareSuffixToDepSecs.get(code);
                            if (depSecs != null)
                                break;
                        }
                    }
                }
                if (depSecs == null)
                    continue;
                delays.add(e.getValue() - (svcEpoch + depSecs));
            }
            if (delays.isEmpty())
                continue;

            java.util.Collections.sort(delays);
            long median = delays.get(delays.size() / 2);

            // For a single observation: MAD is 0 by definition, so use |median| as the
            // effective spread for ranking and threshold purposes.
            long mad = delays.size() == 1
                    ? Math.abs(median)
                    : delays.stream().map(d -> Math.abs(d - median)).sorted()
                            .collect(Collectors.toList()).get(delays.size() / 2);

            // Prefer more observations, then smaller spread, then smallest absolute delay.
            boolean better = delays.size() > bestMatchCount
                    || (delays.size() == bestMatchCount && mad < bestMad);
            if (better) {
                bestMad = mad;
                bestMatchCount = delays.size();
                best = new TrajectoryMatch(meta, median);
            }
        }
        return (best == null || bestMad > madThreshold) ? null : best;
    }

    /**
     * Builds a GTFS-RT {@code SCHEDULED} entity for a vehicle trajectory matched to
     * a
     * theoretical trip. Stops with trajectory observations use those times directly
     * (past/current)
     * or are propagated from the last real-time anchor (future). Stops without
     * observations are
     * interpolated from the anchor or estimated with the median delay.
     */
    private GtfsRealtime.FeedEntity buildEntityFromTrajectory(
            Map<Integer, Long> trajectory,
            TripFinder.TripMeta tripMeta,
            ReferenceStopData refData,
            long medianDelay,
            String lineId,
            Integer directionId,
            long serviceDayStartEpoch) {

        List<String> stopRows = TripFinder.getAllStopTimesFromTrip(tripMeta.tripId);
        if (stopRows.isEmpty())
            return null;

        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(tripMeta.tripId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();
        int resolvedDir = resolveDirectionId(tripMeta, directionId, tripMeta.tripId);
        String routeId = tripMeta.routeId != null ? tripMeta.routeId : lineId;

        GtfsRealtime.TripDescriptor.Builder tripDescriptor = tripUpdate.getTripBuilder()
                .setTripId(tripMeta.tripId)
                .setRouteId(routeId)
                .setDirectionId(resolvedDir)
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED);
        if (tripMeta.startDate != null && !tripMeta.startDate.isEmpty()) {
            tripDescriptor.setStartDate(tripMeta.startDate);
        }

        long lastRealTime = Long.MIN_VALUE;
        long lastTheoRef = Long.MIN_VALUE;
        int seq = 1;

        for (String row : stopRows) {
            String[] parts = row.split(",", 4);
            if (parts.length < 4)
                continue;
            String stopId = parts[0];
            long theoreticalArr = parseSec(parts[1], serviceDayStartEpoch);
            long theoreticalDep = parseSec(parts[2], serviceDayStartEpoch);
            long theoreticalRef = theoreticalDep != Long.MIN_VALUE ? theoreticalDep : theoreticalArr;

            // Look up trajectory time via reference trip's stop_seq for this stop_id
            Integer refSeq = refData.stopIdToSeq().get(stopId);
            Long rtTime = refSeq != null ? trajectory.get(refSeq) : null;

            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stu = tripUpdate.addStopTimeUpdateBuilder();
            stu.setStopSequence(seq++);
            stu.setStopId(stopId);

            if (rtTime != null) {
                if (rtTime <= currentEpochSecond || lastRealTime == Long.MIN_VALUE) {
                    // Past/current observation or first future match — use as anchor
                    stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                            .setTime(rtTime).build());
                    stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                            .setTime(rtTime).build());
                    lastRealTime = rtTime;
                    lastTheoRef = theoreticalRef;
                } else {
                    // Future match after anchor — propagate constant delay to avoid growing-delay
                    if (theoreticalArr != Long.MIN_VALUE)
                        stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                                .setTime(lastRealTime + (theoreticalArr - lastTheoRef)).build());
                    if (theoreticalDep != Long.MIN_VALUE)
                        stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                                .setTime(lastRealTime + (theoreticalDep - lastTheoRef)).build());
                }
            } else if (lastRealTime != Long.MIN_VALUE && theoreticalRef != Long.MIN_VALUE) {
                // No trajectory data but anchor is established — interpolate
                if (theoreticalArr != Long.MIN_VALUE)
                    stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                            .setTime(lastRealTime + (theoreticalArr - lastTheoRef)).build());
                if (theoreticalDep != Long.MIN_VALUE)
                    stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                            .setTime(lastRealTime + (theoreticalDep - lastTheoRef)).build());
            } else {
                // Before first anchor — estimate with median delay
                if (theoreticalArr != Long.MIN_VALUE)
                    stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                            .setTime(theoreticalArr + medianDelay).build());
                if (theoreticalDep != Long.MIN_VALUE)
                    stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                            .setTime(theoreticalDep + medianDelay).build());
            }
        }

        if (tripUpdate.getStopTimeUpdateCount() == 0)
            return null;
        return entityBuilder.build();
    }

    // ─────────────────────────────────────────────────────────────────────────────────────

    /**
     * Processes all blacklisted operator entities together, grouped by line and
     * direction.
     *
     * <p>
     * Uses a group-points approach: all SIRI calls for a line+direction are
     * flattened into {@link CallData} records keyed by reference stop-sequence.
     * Distinct
     * vehicles are identified by detecting descents in the minimum-time envelope
     * across
     * stop sequences, using theoretical inter-stop travel times to avoid false
     * splits.
     * (FIFO ranking). Each extracted trajectory is then matched to the theoretical
     * trip with
     * the lowest Median Absolute Deviation of delays.
     *
     * @param blacklistedEntities all EstimatedVehicleJourney entities from
     *                            blacklisted operators
     * @param entitiesTrips       optional debug map
     * @param startIndex          base index for ordering (placed after normal
     *                            entities)
     * @return list of built GTFS-RT entities
     */
    private List<IndexedEntity> processAllBlacklistedEntities(
            List<JsonNode> blacklistedEntities,
            Map<String, JsonNode> entitiesTrips,
            int startIndex) {

        if (blacklistedEntities.isEmpty()) {
            System.out.println("Blacklisted entities: 0 found, skipping.");
            return List.of();
        }

        System.out.println("Blacklisted entities: processing " + blacklistedEntities.size() + " entities...");

        // Group entities by lineId + directionId so calls from the same line/direction
        // are pooled together regardless of which EstimatedVehicleJourney they came
        // from.
        Map<String, List<JsonNode>> entitiesByLineDirection = new java.util.LinkedHashMap<>();
        for (JsonNode entity : blacklistedEntities) {
            String lineId = "IDFM:" + entity.get("LineRef").get(FIELD_VALUE).asText().split(":")[3];
            int direction = determineDirection(entity);
            String key = lineId + "|" + (direction != -1 ? String.valueOf(direction) : "");
            entitiesByLineDirection.computeIfAbsent(key, k -> new ArrayList<>()).add(entity);
        }

        java.util.concurrent.atomic.AtomicInteger entityIndex = new java.util.concurrent.atomic.AtomicInteger(
                startIndex);
        List<IndexedEntity> results = java.util.Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors()));
        List<Future<?>> futures = new ArrayList<>(entitiesByLineDirection.size());

        for (Map.Entry<String, List<JsonNode>> groupEntry : entitiesByLineDirection.entrySet()) {
            String[] keyParts = groupEntry.getKey().split("\\|", 2);
            String lineId = keyParts[0];
            Integer directionId = (keyParts.length > 1 && !keyParts[1].isEmpty())
                    ? Integer.parseInt(keyParts[1])
                    : null;
            List<JsonNode> groupEntities = groupEntry.getValue();

            futures.add(executor.submit(() -> {
                // ── Step 1: fetch theoretical trips ──────────────────────────────────────
                List<TripFinder.TripMeta> activeTrips;
                try {
                    activeTrips = TripFinder.getActiveTripsForRoutesToday(java.util.List.of(lineId));
                } catch (Exception e) {
                    logger.error("Error fetching active trips for {}: {}", lineId, e.getMessage());
                    return;
                }
                List<TripFinder.TripMeta> dirTrips = activeTrips.stream()
                        .filter(m -> directionId == null || m.directionId == directionId)
                        .collect(Collectors.toList());
                if (dirTrips.isEmpty())
                    return;

                // ── Step 2: build reference stop data from the longest trip ──────────────
                ReferenceStopData refData = buildReferenceStopData(dirTrips, directionId);
                if (refData == null || refData.codeToStopId().isEmpty())
                    return;

                // ── Step 3: flatten SIRI calls → CallData list ───────────────────────────
                long midnightEpoch = java.time.LocalDate.now(ZONE_ID).atStartOfDay(ZONE_ID).toEpochSecond();
                List<CallData> allCalls = flattenToCallData(groupEntities, refData, midnightEpoch);
                if (allCalls.isEmpty())
                    return;

                // ── Step 4: group calls into distinct vehicle groups (minimum-envelope) ───
                Map<Integer, List<CallData>> groups = groupPoints(allCalls, refData.seqToInterStopMinutes());

                // Convert each group to a trajectory map (seq → epoch) for MAD matching.
                // Groups are already ordered earliest-first by construction (first seq sorted
                // ascending, descent-detected groups appended in sequence order).
                List<Map<Integer, Long>> trajectories = new ArrayList<>();
                for (List<CallData> groupCalls : groups.values()) {
                    Map<Integer, Long> traj = new java.util.LinkedHashMap<>();
                    for (CallData cd : groupCalls) {
                        long epoch = cd.departureEpoch() != Long.MIN_VALUE ? cd.departureEpoch() : cd.arrivalEpoch();
                        if (epoch != Long.MIN_VALUE)
                            traj.putIfAbsent(cd.stopSeq(), epoch);
                    }
                    if (!traj.isEmpty())
                        trajectories.add(traj);
                }

                // MAD threshold: 5 min for metro/tram, 8 min for bus
                String routeForLookup = dirTrips.get(0).routeId != null
                        ? dirTrips.get(0).routeId
                        : lineId;
                int routeType = TripFinder.getRouteType(routeForLookup);
                long madThreshold = (routeType == 0 || routeType == 1) ? 300L : 480L;

                Set<String> usedTripIds = new java.util.HashSet<>();
                int localCount = 0;

                // ── Step 5: match each trajectory to a theoretical trip ───────────────────
                for (Map<Integer, Long> trajectory : trajectories) {
                    TrajectoryMatch match = matchTrajectoryToTrip(
                            trajectory, dirTrips, refData, madThreshold, usedTripIds);
                    if (match == null)
                        continue;

                    TripFinder.TripMeta tripMeta = match.tripMeta();
                    usedTripIds.add(tripMeta.tripId);

                    long serviceDayStartEpoch;
                    try {
                        java.time.LocalDate svcDay = (tripMeta.startDate != null && !tripMeta.startDate.isEmpty())
                                ? java.time.LocalDate.parse(tripMeta.startDate,
                                        java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))
                                : java.time.LocalDate.now(ZONE_ID);
                        serviceDayStartEpoch = svcDay.atStartOfDay(ZONE_ID).toEpochSecond();
                    } catch (Exception e) {
                        serviceDayStartEpoch = java.time.LocalDate.now(ZONE_ID).atStartOfDay(ZONE_ID).toEpochSecond();
                    }

                    GtfsRealtime.FeedEntity entity = buildEntityFromTrajectory(
                            trajectory, tripMeta, refData, match.medianDelay(),
                            lineId, directionId, serviceDayStartEpoch);
                    if (entity == null)
                        continue;

                    // Update match cache
                    Integer lastStopSec = TripFinder.getLastStopTime(tripMeta.tripId);
                    long lastStopEpoch = (lastStopSec != null) ? serviceDayStartEpoch + lastStopSec : 0L;
                    blacklistedMatchCache.put(tripMeta.tripId, new BlacklistedTripCache(
                            tripMeta.tripId, match.medianDelay(), lastStopEpoch, currentEpochSecond));

                    if (entitiesTrips != null) {
                        entitiesTrips.put("BL-" + tripMeta.tripId, groupEntities.get(0));
                    }
                    results.add(new IndexedEntity(entityIndex.getAndIncrement(), entity));
                    localCount++;
                }

                // ── Step 6: re-emit cached trips not matched this cycle ───────────────────
                for (TripFinder.TripMeta tripMeta : dirTrips) {
                    if (usedTripIds.contains(tripMeta.tripId))
                        continue;
                    BlacklistedTripCache cached = blacklistedMatchCache.get(tripMeta.tripId);
                    if (cached == null)
                        continue;

                    long serviceDayStartEpoch;
                    try {
                        java.time.LocalDate svcDay = (tripMeta.startDate != null && !tripMeta.startDate.isEmpty())
                                ? java.time.LocalDate.parse(tripMeta.startDate,
                                        java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))
                                : java.time.LocalDate.now(ZONE_ID);
                        serviceDayStartEpoch = svcDay.atStartOfDay(ZONE_ID).toEpochSecond();
                    } catch (Exception e) {
                        serviceDayStartEpoch = java.time.LocalDate.now(ZONE_ID).atStartOfDay(ZONE_ID).toEpochSecond();
                    }

                    GtfsRealtime.FeedEntity cachedEntity = buildCachedBlacklistedTripEntity(
                            tripMeta, directionId, lineId, serviceDayStartEpoch, cached.medianDelay);
                    if (cachedEntity != null) {
                        results.add(new IndexedEntity(entityIndex.getAndIncrement(), cachedEntity));
                        localCount++;
                    }
                }

                System.out.println("Blacklisted line " + lineId + ", dir " + directionId
                        + ": " + groupEntities.size() + " entities → "
                        + trajectories.size() + " trajectories → " + localCount + " trips matched");
            }));
        }

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted during blacklisted entity parallel processing", e);
        } catch (ExecutionException e) {
            logger.error("Execution error during blacklisted entity processing", e);
        } finally {
            shutdownExecutor(executor);
        }

        System.out.println("Blacklisted entities: " + results.size() + " trips produced (group-points approach).");
        return results;
    }

    /**
     * Converts a seconds-of-day string (as stored in stop_times) plus a service-day
     * base epoch into an absolute epoch second.
     * Returns {@code Long.MIN_VALUE} if the string is null, empty, or
     * {@code "null"}.
     */
    private static long parseSec(String secStr, long serviceDayStartEpoch) {
        if (secStr == null || secStr.isEmpty() || secStr.equals("null"))
            return Long.MIN_VALUE;
        try {
            return serviceDayStartEpoch + Long.parseLong(secStr);
        } catch (NumberFormatException e) {
            return Long.MIN_VALUE;
        }
    }

    private GtfsRealtime.FeedEntity buildExtraJourneyFeedEntity(
            String vehicleId,
            String lineId,
            Integer directionId,
            List<JsonNode> estimatedCalls) {

        String syntheticId = "EJ-" + vehicleId;

        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId(syntheticId);

        GtfsRealtime.TripUpdate.Builder tripUpdate = entityBuilder.getTripUpdateBuilder();

        GtfsRealtime.TripDescriptor.Builder tripDescriptor = tripUpdate.getTripBuilder()
                .setTripId(syntheticId)
                .setRouteId(lineId)
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.ADDED);
        if (directionId != null) {
            tripDescriptor.setDirectionId(directionId);
        }

        int seq = 1;
        for (JsonNode call : estimatedCalls) {
            if (isEstimatedCallInPast(call))
                continue;
            if (!call.has(FIELD_STOP_POINT_REF))
                continue;

            String stopCode = call.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":")[3];
            String stopId = TripFinder.resolveStopId(stopCode);
            if (stopId == null)
                continue;

            boolean isDepartureCancelled = call.has(FIELD_DEPARTURE_STATUS)
                    && (call.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_CANCELLED)
                            || call.get(FIELD_DEPARTURE_STATUS).asText().contains(STATUS_MISSED));
            boolean isArrivalCancelled = call.has(FIELD_ARRIVAL_STATUS)
                    && (call.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_CANCELLED)
                            || call.get(FIELD_ARRIVAL_STATUS).asText().contains(STATUS_MISSED));

            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stu = tripUpdate.addStopTimeUpdateBuilder();
            stu.setStopSequence(seq++);
            stu.setStopId(stopId);

            if (isDepartureCancelled || isArrivalCancelled) {
                stu.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
                continue;
            }

            if (call.has(FIELD_EXPECTED_ARRIVAL_TIME)) {
                long t = parseTime(call.get(FIELD_EXPECTED_ARRIVAL_TIME).asText());
                if (t != Long.MIN_VALUE)
                    stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(t).build());
            } else if (call.has(FIELD_AIMED_ARRIVAL_TIME)) {
                long t = parseTime(call.get(FIELD_AIMED_ARRIVAL_TIME).asText());
                if (t != Long.MIN_VALUE)
                    stu.setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(t).build());
            }
            if (call.has(FIELD_EXPECTED_DEPARTURE_TIME)) {
                long t = parseTime(call.get(FIELD_EXPECTED_DEPARTURE_TIME).asText());
                if (t != Long.MIN_VALUE)
                    stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(t).build());
            } else if (call.has(FIELD_AIMED_DEPARTURE_TIME)) {
                long t = parseTime(call.get(FIELD_AIMED_DEPARTURE_TIME).asText());
                if (t != Long.MIN_VALUE)
                    stu.setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(t).build());
            }
        }

        if (tripUpdate.getStopTimeUpdateCount() == 0) {
            return null;
        }

        return entityBuilder.build();
    }

    /**
     * Writes the completed GTFS-RT feed to a Protocol Buffer file.
     *
     * @param feedMessage the GTFS-RT feed message builder containing all entities
     * @param filePath    the output file path for the .pb file
     */
    private void writeFeedToFile(GtfsRealtime.FeedMessage.Builder feedMessage, String filePath) {
        writeFeedToFile(feedMessage.build(), filePath);
    }

    /**
     * Writes a built GTFS-RT feed message to a Protocol Buffer file.
     *
     * @param feed     the built GTFS-RT feed message
     * @param filePath the output file path for the .pb file
     */
    private void writeFeedToFile(GtfsRealtime.FeedMessage feed, String filePath) {
        java.nio.file.Path target = java.nio.file.Paths.get(filePath);
        java.nio.file.Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (java.io.FileOutputStream outputStream = new java.io.FileOutputStream(tmp.toFile())) {
            feed.writeTo(outputStream);
        } catch (java.io.IOException e) {
            logger.error("Error writing GTFS-RT feed: {}", e.getMessage(), e);
            return;
        }
        try {
            java.nio.file.Files.move(tmp, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            System.out.println("GTFS-RT feed written to " + filePath);
        } catch (java.io.IOException e) {
            logger.error("Error renaming GTFS-RT feed file: {}", e.getMessage(), e);
        }
    }

    /**
     * Returns a copy of the feed where every REPLACEMENT trip is converted to
     * ADDED.
     * Used to produce the main feed at /gtfs-rt-trips-idfm while keeping the beta
     * feed (with REPLACEMENT) at /gtfs-rt-trips-idfm-beta.
     *
     * @param original the source feed (may contain REPLACEMENT entries)
     * @return a new FeedMessage with REPLACEMENT replaced by ADDED
     */
    @SuppressWarnings("unused")
    private Set<String> extractExistingEntityIds(GtfsRealtime.FeedMessage.Builder feedMessage) {
        Set<String> ids = new java.util.HashSet<>();
        for (int i = 0; i < feedMessage.getEntityCount(); i++) {
            ids.add(feedMessage.getEntity(i).getId());
        }
        return ids;
    }

    @SuppressWarnings("unused")
    private Map<String, Map<Integer, List<TripFinder.TripMeta>>> groupTheoreticalTripsByRouteAndDirection(
            List<TripFinder.TripMeta> trips) {
        Map<String, Map<Integer, List<TripFinder.TripMeta>>> result = new java.util.LinkedHashMap<>();
        for (TripFinder.TripMeta trip : trips) {
            result.computeIfAbsent(trip.routeId, k -> new java.util.LinkedHashMap<>())
                  .computeIfAbsent(trip.directionId, k -> new ArrayList<>())
                  .add(trip);
        }
        return result;
    }

    // =========================================================================
    // Platform GTFS-RT feed
    // =========================================================================

    /**
     * Generates a GTFS-RT platform feed that mirrors {@code mainFeed} exactly
     * (same number of entities, same trips, same schedule relationships) but
     * overrides each StopTimeUpdate's {@code stop_id} with the specific quay
     * stop ID whenever the SIRI Lite {@code ExpectedQuayRef} field is present.
     *
     * <p>Trips for which no current SIRI data is available (re-emitted cached
     * trips, etc.) are copied from the main feed without modification.
     *
     * @param mainFeed     the already-built main GTFS-RT feed
     * @param siriLiteData the same SIRI Lite payload used to build the main feed
     */
    private void generatePlatformFeed(GtfsRealtime.FeedMessage mainFeed, JsonNode siriLiteData) {
        // Build vehicleId → SIRI entity map for fast lookup
        Map<String, JsonNode> vehicleToEntity = new HashMap<>();
        for (JsonNode entity : extractEntitiesFromSiriLite(siriLiteData)) {
            String vehicleId = entity.path("DatedVehicleJourneyRef").path(FIELD_VALUE).asText(null);
            if (vehicleId != null) vehicleToEntity.put(vehicleId, entity);
        }

        // Invert vehicleToTrip → tripId → vehicleId (keep first if collision)
        Map<String, String> tripToVehicle = new HashMap<>();
        vehicleToTrip.forEach((vehicleId, tripId) -> tripToVehicle.putIfAbsent(tripId, vehicleId));

        GtfsRealtime.FeedMessage.Builder platformFeed = mainFeed.toBuilder().clearEntity();

        for (GtfsRealtime.FeedEntity feedEntity : mainFeed.getEntityList()) {
            if (!feedEntity.hasTripUpdate()) {
                platformFeed.addEntity(feedEntity);
                continue;
            }

            String tripId = feedEntity.getTripUpdate().getTrip().getTripId();
            String vehicleId = tripToVehicle.get(tripId);
            JsonNode siriEntity = (vehicleId != null) ? vehicleToEntity.get(vehicleId) : null;

            if (siriEntity == null) {
                // Re-emitted or synthetic trip: no current SIRI data → copy as-is
                platformFeed.addEntity(feedEntity);
            } else {
                platformFeed.addEntity(rebuildWithPlatformStops(feedEntity, siriEntity, tripId));
            }
        }

        GtfsRealtime.FeedMessage built = platformFeed.build();

        writeFeedToFile(built, "gtfs-rt-trips-idfm.pb");
        System.out.println("[Trips] GTFS-RT feed written (" + built.getEntityCount() + " entities, with platform assignments)");
    }

    /**
     * Returns a copy of {@code original} where each StopTimeUpdate gains a
     * {@code stop_time_properties.assigned_stop_id} set to the quay stop ID
     * from {@code ExpectedQuayRef} when one is resolvable.
     *
     * <p>{@code stop_id} is intentionally left unchanged so that trip-matching
     * against the static GTFS stop_times continues to work correctly.
     * {@code assigned_stop_id} is the GTFS-RT field specifically designed for
     * real-time platform/track assignments.
     *
     * <p>All other fields (times, schedule relationship, vehicle, etc.) are
     * preserved unchanged.
     */
    private GtfsRealtime.FeedEntity rebuildWithPlatformStops(
            GtfsRealtime.FeedEntity original, JsonNode siriEntity, String tripId) {

        // Build stop_sequence → quay stop_id map from SIRI estimated calls
        Map<Integer, String> seqToQuayId = buildQuayStopIdMap(siriEntity, tripId);
        if (seqToQuayId.isEmpty()) return original;

        GtfsRealtime.TripUpdate tu = original.getTripUpdate();
        GtfsRealtime.TripUpdate.Builder tuBuilder = tu.toBuilder().clearStopTimeUpdate();
        for (GtfsRealtime.TripUpdate.StopTimeUpdate stu : tu.getStopTimeUpdateList()) {
            String quayId = seqToQuayId.get(stu.getStopSequence());
            if (quayId != null) {
                tuBuilder.addStopTimeUpdate(stu.toBuilder()
                        .setStopTimeProperties(
                                GtfsRealtime.TripUpdate.StopTimeUpdate.StopTimeProperties.newBuilder()
                                        .setAssignedStopId(quayId))
                        .build());
            } else {
                tuBuilder.addStopTimeUpdate(stu);
            }
        }
        return original.toBuilder().setTripUpdate(tuBuilder).build();
    }

    /**
     * Builds a map of {@code stop_sequence → quay stop_id} by scanning a SIRI
     * entity's EstimatedCalls for {@code ExpectedQuayRef} fields.
     *
     * <p>The stop sequence is resolved the same way as the main feed (via
     * {@link TripFinder#findStopSequence}), so cache hits are virtually free.
     */
    private Map<Integer, String> buildQuayStopIdMap(JsonNode siriEntity, String tripId) {
        Map<Integer, String> result = new HashMap<>();

        List<JsonNode> calls = getSortedEstimatedCalls(siriEntity);
        List<String> processedSeqs = new ArrayList<>();

        for (JsonNode call : calls) {
            String quayStopId = resolveQuayStopId(call);
            if (quayStopId == null) continue;

            if (!call.has(FIELD_STOP_POINT_REF)) continue;
            String[] parts = call.get(FIELD_STOP_POINT_REF).get(FIELD_VALUE).asText().split(":");
            if (parts.length < 4) continue;

            String regularStopId = TripFinder.resolveStopId(parts[3]);
            if (regularStopId == null) continue;

            String seq = TripFinder.findStopSequence(tripId, regularStopId, processedSeqs);
            if (seq == null) continue;
            processedSeqs.add(seq);

            result.put(Integer.parseInt(seq), quayStopId);
        }
        return result;
    }

    /**
     * Extracts the quay stop ID from a SIRI EstimatedCall using two strategies:
     *
     * <ol>
     *   <li><b>Primary</b>: {@code DepartureStopAssignment} or {@code ArrivalStopAssignment}
     *       containing an {@code ExpectedQuayRef}. Format:
     *       {@code STIF:StopPoint:Q:471581:} → {@code IDFM:471581}. Constructed
     *       directly without a DB lookup.</li>
     *   <li><b>Fallback</b>: {@code DeparturePlatformName} or {@code ArrivalPlatformName}
     *       combined with the {@code StopPointRef} to find a sibling quay in the DB
     *       that shares the same parent station and has the matching
     *       {@code platform_code}.</li>
     * </ol>
     */
    private String resolveQuayStopId(JsonNode estimatedCall) {
        for (String field : new String[]{FIELD_DEPARTURE_STOP_ASSIGNMENT, FIELD_ARRIVAL_STOP_ASSIGNMENT}) {
            JsonNode quayRef = estimatedCall.path(field).path(FIELD_EXPECTED_QUAY_REF).path(FIELD_VALUE);
            if (quayRef.isMissingNode()) continue;
            String[] parts = quayRef.asText().split(":");
            if (parts.length >= 4 && parts[3].matches("\\d+")) {
                return "IDFM:" + parts[3];
            }
        }

        // Fallback: resolve via DeparturePlatformName / ArrivalPlatformName
        if (!estimatedCall.has(FIELD_STOP_POINT_REF)) return null;
        String[] stopParts = estimatedCall.path(FIELD_STOP_POINT_REF).path(FIELD_VALUE).asText("").split(":");
        if (stopParts.length < 4 || stopParts[3].isBlank()) return null;
        String stopCode = stopParts[3];

        // StopPointRef is already a quay (StopPoint:Q:) → return it directly
        if (stopParts.length >= 3 && "Q".equals(stopParts[2]) && stopCode.matches("\\d+")) {
            return "IDFM:" + stopCode;
        }

        for (String platformField : new String[]{FIELD_DEPARTURE_PLATFORM_NAME, FIELD_ARRIVAL_PLATFORM_NAME}) {
            String platformCode = estimatedCall.path(platformField).path(FIELD_VALUE).asText("").trim();
            if (platformCode.isEmpty()) continue;
            String quayId = TripFinder.findStopByParentAndPlatformCode(stopCode, platformCode);
            if (quayId != null) return quayId;
        }

        return null;
    }

    private GtfsRealtime.FeedMessage convertReplacementToAdded(GtfsRealtime.FeedMessage original) {
        GtfsRealtime.FeedMessage.Builder builder = original.toBuilder();
        for (int i = 0; i < builder.getEntityCount(); i++) {
            GtfsRealtime.FeedEntity entity = builder.getEntity(i);
            if (entity.hasTripUpdate()) {
                GtfsRealtime.TripUpdate tu = entity.getTripUpdate();
                if (tu.hasTrip() && tu.getTrip()
                        .getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.REPLACEMENT) {
                    builder.setEntity(i, entity.toBuilder()
                            .setTripUpdate(tu.toBuilder()
                                    .setTrip(tu.getTrip().toBuilder()
                                            .setScheduleRelationship(
                                                    GtfsRealtime.TripDescriptor.ScheduleRelationship.ADDED)))
                            .build());
                }
            }
        }
        return builder.build();
    }
}