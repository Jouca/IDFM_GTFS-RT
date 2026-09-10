package org.jouca.idfm_gtfs_rt.generator;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jouca.idfm_gtfs_rt.fetchers.AlertFetcher;
import org.jouca.idfm_gtfs_rt.records.StopClosure;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.transit.realtime.GtfsRealtime;

/**
 * Generator component responsible for creating GTFS-Realtime alert feeds from IDFM disruption data.
 * 
 * <p>This class fetches disruption information from the IDFM (Île-de-France Mobilités) API,
 * processes the data, and generates a GTFS-Realtime compliant Protocol Buffer file containing
 * service alerts. The alerts include information about service disruptions, construction work,
 * and other incidents affecting transit services.</p>
 * 
 * <p>The generated alerts include:</p>
 * <ul>
 *   <li>Active time periods for each disruption</li>
 *   <li>Affected routes and stops (informed entities)</li>
 *   <li>Cause and effect of the disruption</li>
 *   <li>Severity level and descriptive text</li>
 * </ul>
 * 
 * @author Jouca
 * @since 1.0
 */
@Component
public class AlertGenerator {

    @Autowired
    private ElevatorAlertGenerator elevatorAlertGenerator;
    
    private static final String FIELD_CAUSE = "cause";
    private static final String FIELD_SEVERITY = "severity";
    private static final String FIELD_TITLE = "title";
    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_IMPACTED_OBJECTS = "impactedObjects";
    private static final String FIELD_IMPACTED_SECTIONS = "impactedSections";
    private static final String FIELD_APPLICATION_PERIODS = "applicationPeriods";
    private static final String FIELD_LAST_UPDATE = "lastUpdate";
    
    /**
     * Creates a GTFS-Realtime TimeRange from an application period JSON node.
     *
     * @param applicationPeriod JSON node containing begin and end timestamps
     * @return a TimeRange.Builder with start and end times set
     */
    private GtfsRealtime.TimeRange.Builder createTimeRange(JsonNode applicationPeriod) {
        String startStr = applicationPeriod.get("begin").asText();
        String endStr = applicationPeriod.get("end").asText();
        
        long startEpoch = convertToEpoch(startStr);
        long endEpoch = convertToEpoch(endStr);
        
        GtfsRealtime.TimeRange.Builder timeRange = GtfsRealtime.TimeRange.newBuilder();
        timeRange.setStart(startEpoch);
        timeRange.setEnd(endEpoch);
        
        return timeRange;
    }
    
    /**
     * Maps IDFM cause strings to GTFS-Realtime Cause enum values.
     *
     * @param cause the IDFM cause string (e.g., "TRAVAUX", "PERTURBATION")
     * @return the corresponding GTFS-Realtime Cause enum value
     */
    private GtfsRealtime.Alert.Cause mapCause(String cause) {
        if (cause == null) {
            return GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE;
        }
        
        switch (cause) {
            case "TRAVAUX":
                return GtfsRealtime.Alert.Cause.CONSTRUCTION;
            case "PERTURBATION":
                return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            default:
                return GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE;
        }
    }
    
    /**
     * Maps IDFM severity strings to GTFS-Realtime Effect enum values.
     *
     * @param severity the IDFM severity string (e.g., "BLOQUANTE", "PERTURBEE")
     * @return the corresponding GTFS-Realtime Effect enum value
     */
    private GtfsRealtime.Alert.Effect mapEffect(String severity) {
        if (severity == null) {
            return GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT;
        }
        
        switch (severity) {
            case "BLOQUANTE":
                return GtfsRealtime.Alert.Effect.NO_SERVICE;
            case "PERTURBEE":
                return GtfsRealtime.Alert.Effect.REDUCED_SERVICE;
            default:
                return GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT;
        }
    }
    
    /**
     * Maps IDFM severity strings to GTFS-Realtime SeverityLevel enum values.
     *
     * @param severity the IDFM severity string (e.g., "BLOQUANTE", "PERTURBEE")
     * @return the corresponding GTFS-Realtime SeverityLevel enum value
     */
    private GtfsRealtime.Alert.SeverityLevel mapSeverityLevel(String severity) {
        if (severity == null) {
            return GtfsRealtime.Alert.SeverityLevel.UNKNOWN_SEVERITY;
        }
        
        switch (severity) {
            case "BLOQUANTE":
                return GtfsRealtime.Alert.SeverityLevel.SEVERE;
            case "PERTURBEE":
                return GtfsRealtime.Alert.SeverityLevel.WARNING;
            default:
                return GtfsRealtime.Alert.SeverityLevel.UNKNOWN_SEVERITY;
        }
    }
    
    /**
     * Checks if a disruption ID is present in the impacted object's disruption IDs.
     *
     * @param impactedObject JSON node containing disruption IDs
     * @param disruptionId the disruption ID to search for
     * @return true if the disruption ID is found, false otherwise
     */
    private boolean isDisruptionInImpactedObject(JsonNode impactedObject, String disruptionId) {
        ArrayNode disruptionIds = (ArrayNode) impactedObject.get("disruptionIds");
        for (int i = 0; i < disruptionIds.size(); i++) {
            if (disruptionId.equals(disruptionIds.get(i).asText())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Strips the leading namespace segment off an IDFM-prefixed id to get the plain GTFS id
     * (e.g. "stop_point:IDFM:11341" -&gt; "IDFM:11341", "line:IDFM:C01563" -&gt; "IDFM:C01563").
     * IDFM prefixes ids this way consistently, both on impacted objects and on the top-level
     * "id" field of entries in the disruption API's "lines" array.
     *
     * @param prefixedId the raw IDFM id
     * @return the plain GTFS id
     */
    private String stripNamespace(String prefixedId) {
        String[] idParts = prefixedId.split(":");
        return String.join(":", Arrays.copyOfRange(idParts, 1, idParts.length)).replace("\"", "");
    }

    /**
     * Extracts the GTFS id (route or stop id) embedded in an impacted object's IDFM id.
     *
     * @param impactedObject JSON node containing impacted object information
     * @return the extracted GTFS id
     */
    private String extractImpactedObjectId(JsonNode impactedObject) {
        return stripNamespace(impactedObject.get("id").asText());
    }

    /**
     * How a single disruption impacts a single route: either the whole line is down
     * ({@code lineImpacted}, with no specific stops named), a non-empty set of specific stops on
     * that line is closed/skipped ({@code stopIds}), or one or more "no service between X and Y"
     * boundaries apply ({@code sections}) — see {@link #extractSectionsForRoute}.
     *
     * @param routeId      the GTFS route id
     * @param lineImpacted whether the line itself was tagged as impacted by this disruption
     * @param stopIds      the specific stops on this line tagged as impacted by this disruption
     * @param sections     "no service between two stations" boundaries for this route/disruption
     */
    private record RouteImpact(String routeId, boolean lineImpacted, List<String> stopIds,
            List<StopClosure.Section> sections) {
    }

    /**
     * Extracts the "no service between X and Y" boundaries that apply to a specific route from a
     * disruption's raw {@code impactedSections} array.
     *
     * @param impactedSections the disruption's impactedSections array, or {@code null} if absent
     * @param routeId          the GTFS route id to filter sections down to
     * @return the matching sections, possibly empty
     */
    private List<StopClosure.Section> extractSectionsForRoute(ArrayNode impactedSections, String routeId) {
        List<StopClosure.Section> sections = new ArrayList<>();
        if (impactedSections == null) {
            return sections;
        }
        for (JsonNode section : impactedSections) {
            if (!routeId.equals(stripNamespace(section.get("lineId").asText()))) {
                continue;
            }
            String fromId = stripNamespace(section.get("from").get("id").asText());
            String toId = stripNamespace(section.get("to").get("id").asText());
            sections.add(new StopClosure.Section(fromId, toId));
        }
        return sections;
    }

    /**
     * Determines, for a given disruption, how each line is impacted: as a whole (no specific
     * stops named), via a specific set of closed/skipped stops, or via one or more section
     * boundaries.
     * <p>
     * IDFM disruptions tag the parent line as impacted alongside the specific stops that are
     * actually closed/skipped (e.g. a construction closure of a couple of stops on a line).
     * Distinguishing the two lets callers avoid treating a stop-level closure as if it applied
     * to the entire route. For a severe disruption ("no service between X and Y, delayed on the
     * rest of the line"), IDFM's per-stop list has been observed to enumerate most or all of the
     * line's stations rather than just the closed ones — {@code sections}, sourced from the
     * disruption's structured {@code impactedSections} field, is the reliable way to know exactly
     * which segment has no service in that case.
     *
     * @param disruptionId     the disruption ID to match against
     * @param lines            map of line data
     * @param impactedSections the disruption's impactedSections array, or {@code null} if absent
     * @return one {@link RouteImpact} per line referenced by this disruption
     */
    private List<RouteImpact> computeRouteImpacts(String disruptionId, Map<String, Object> lines,
            ArrayNode impactedSections) {
        List<RouteImpact> impacts = new ArrayList<>();

        for (Map.Entry<String, Object> lineEntry : lines.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> line = (Map<String, Object>) lineEntry.getValue();

            boolean lineImpacted = false;
            List<String> impactedStopIds = new ArrayList<>();

            for (JsonNode impactedObject : (ArrayNode) line.get(FIELD_IMPACTED_OBJECTS)) {
                if (!isDisruptionInImpactedObject(impactedObject, disruptionId)) {
                    continue;
                }

                String type = impactedObject.get("type").asText();
                switch (type) {
                    case "line":
                        lineImpacted = true;
                        break;
                    case "stop_point", "stop_area":
                        impactedStopIds.add(extractImpactedObjectId(impactedObject));
                        break;
                    default:
                        break;
                }
            }

            String routeId = stripNamespace((String) line.get("id"));
            List<StopClosure.Section> sections = extractSectionsForRoute(impactedSections, routeId);

            if (lineImpacted || !impactedStopIds.isEmpty() || !sections.isEmpty()) {
                impacts.add(new RouteImpact(routeId, lineImpacted, impactedStopIds, sections));
            }
        }

        return impacts;
    }

    /**
     * Adds informed entities to the alert builder based on lines and their impacted objects.
     * <p>
     * To avoid an alert whose effect (e.g. NO_SERVICE) reads as applying to the entire route
     * when only specific stops are actually closed, each affected stop is paired with its
     * route in a single EntitySelector. A bare route-only selector is emitted when the line
     * itself is impacted with no specific stops named (a genuinely line-wide disruption), or
     * when the impact is described via section boundaries: IDFM's per-stop list can't be
     * trusted on its own in that case (see {@link #computeRouteImpacts}), and GTFS-Realtime has
     * no "no service between X and Y" selector to fall back on more precisely here.
     *
     * @param alertBuilder the alert builder to add informed entities to
     * @param disruptionId the disruption ID to match against
     * @param lines map of line data
     * @param impactedSections the disruption's impactedSections array, or {@code null} if absent
     */
    private void addInformedEntities(GtfsRealtime.Alert.Builder alertBuilder, String disruptionId,
            Map<String, Object> lines, ArrayNode impactedSections) {
        for (RouteImpact impact : computeRouteImpacts(disruptionId, lines, impactedSections)) {
            if (!impact.sections().isEmpty()) {
                alertBuilder.addInformedEntityBuilder().setRouteId(impact.routeId());
            } else if (!impact.stopIds().isEmpty()) {
                for (String stopId : impact.stopIds()) {
                    alertBuilder.addInformedEntityBuilder()
                        .setRouteId(impact.routeId())
                        .setStopId(stopId);
                }
            } else if (impact.lineImpacted()) {
                alertBuilder.addInformedEntityBuilder().setRouteId(impact.routeId());
            }
        }
    }
    
    /**
     * Sets alert text fields (header and description) on the alert builder.
     *
     * @param alertBuilder the alert builder to set text fields on
     * @param title the title text (may be null)
     * @param message the description message text
     */
    private void setAlertText(GtfsRealtime.Alert.Builder alertBuilder, String title, String message) {
        if (title != null) {
            alertBuilder.setHeaderText(
                GtfsRealtime.TranslatedString.newBuilder()
                    .addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder().setText(title))
            );
        }
        
        alertBuilder.setDescriptionText(
            GtfsRealtime.TranslatedString.newBuilder()
                .addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder().setText(message))
        );
    }
    
    /**
     * Populates an alert builder with all necessary fields from the alert data.
     *
     * @param alertBuilder the alert builder to populate
     * @param alert map containing alert data
     * @param lines map of line data for informed entities
     */
    private void populateAlertBuilder(GtfsRealtime.Alert.Builder alertBuilder, Map<String, Object> alert, Map<String, Object> lines) {
        String disruptionId = alert.get("id").toString();
        String cause = (String) alert.get(FIELD_CAUSE);
        String severity = (String) alert.get(FIELD_SEVERITY);
        String title = (String) alert.get(FIELD_TITLE);
        String message = alert.get(FIELD_MESSAGE).toString();
        ArrayNode impactedSections = (ArrayNode) alert.get(FIELD_IMPACTED_SECTIONS);

        addInformedEntities(alertBuilder, disruptionId, lines, impactedSections);
        alertBuilder.setCause(mapCause(cause));
        alertBuilder.setEffect(mapEffect(severity));
        alertBuilder.setSeverityLevel(mapSeverityLevel(severity));
        setAlertText(alertBuilder, title, message);
    }
    
    /**
     * Creates a single GTFS-Realtime alert entity for a disruption, carrying all of its
     * application periods as separate active_period entries (per the GTFS-Realtime spec)
     * instead of splitting one disruption into several near-duplicate alert entities.
     *
     * @param feed the feed message builder to add the entity to
     * @param alert map containing alert data
     * @param applicationPeriods JSON array of application periods for this disruption
     * @param lines map of line data for informed entities
     */
    private void createAlertEntity(GtfsRealtime.FeedMessage.Builder feed, Map<String, Object> alert,
                                    ArrayNode applicationPeriods, Map<String, Object> lines) {
        String entityId = alert.get("id").toString();

        GtfsRealtime.Alert.Builder alertBuilder = feed.addEntityBuilder()
            .setId(entityId)
            .getAlertBuilder();

        for (JsonNode applicationPeriod : applicationPeriods) {
            alertBuilder.addActivePeriod(createTimeRange(applicationPeriod));
        }

        populateAlertBuilder(alertBuilder, alert, lines);
    }

    /**
     * Generates a GTFS-Realtime alert feed from IDFM disruption data.
     * 
     * <p>This method performs the following operations:</p>
     * <ol>
     *   <li>Fetches alert data from the IDFM API using {@link AlertFetcher}</li>
     *   <li>Parses disruptions and affected lines from the JSON response</li>
     *   <li>Creates GTFS-Realtime alert entities for each disruption and application period</li>
     *   <li>Maps IDFM-specific fields (cause, severity, effect) to GTFS-Realtime enums</li>
     *   <li>Associates alerts with affected routes and stops</li>
     *   <li>Writes the complete feed to a Protocol Buffer file (gtfs-rt-alerts-idfm.pb)</li>
     * </ol>
     * 
     * <p>Each disruption produces a single alert entity carrying all of its application
     * periods as separate active_period entries.</p>
     * 
     * @throws Exception if there is an error fetching alert data, parsing JSON, or writing the output file
     * @see AlertFetcher#fetchAlertData()
     */
    private void saveAlertsDataToFile(JsonNode data, String filePath) {
        try (java.io.FileOutputStream out = new java.io.FileOutputStream(filePath)) {
            out.write(data.toString().getBytes());
        } catch (java.io.IOException e) {
            System.err.println("Error writing alerts data to " + filePath + ": " + e.getMessage());
        }
    }

    public void generateAlert() throws Exception {
        System.out.println("Fetching alerts from online data...");
        JsonNode siriData = AlertFetcher.fetchAlertData();
        saveAlertsDataToFile(siriData, "alerts_data.json");

        GtfsRealtime.FeedMessage.Builder feed = GtfsRealtime.FeedMessage.newBuilder();

        Map<String, Object> alertDict = parseDisruptions(siriData.get("disruptions"));
        Map<String, Object> lines = parseLines(siriData.get("lines"));

        // One alert entity per disruption, carrying all of its application periods
        for (Map.Entry<String, Object> entry : alertDict.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> alert = (Map<String, Object>) entry.getValue();

            createAlertEntity(feed, alert, (ArrayNode) alert.get(FIELD_APPLICATION_PERIODS), lines);
        }

        // Append elevator outage alerts to the same feed
        elevatorAlertGenerator.addElevatorAlertsToFeed(feed);

        // Build the feed message
        feed.setHeader(GtfsRealtime.FeedHeader.newBuilder()
            .setGtfsRealtimeVersion("2.0")
            .setIncrementality(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET)
            .setTimestamp(System.currentTimeMillis()));

        try (FileOutputStream output = new FileOutputStream("gtfs-rt-alerts-idfm.pb")) {
            feed.build().writeTo(output);
        }

        System.out.println("Alerts generated successfully!");
    }

    /**
     * Computes the set of service closures described by the given IDFM disruption data: for
     * every disruption whose severity maps to {@link GtfsRealtime.Alert.Effect#NO_SERVICE}, one
     * {@link StopClosure} per affected route is returned, carrying the disruption's application
     * periods converted to epoch time windows plus either the specific closed stop ids, one or
     * more "no service between X and Y" sections, or — when IDFM names neither for a route it
     * still tags as impacted — {@code entireRouteClosure=true}, meaning the whole line has no
     * service (e.g. a full-line closure for maintenance work) and every trip on it should be
     * canceled outright rather than having individual stops skipped.
     * <p>
     * This is a pure function of the parsed disruption/line data so it can be exercised directly
     * in tests; {@link #getActiveStopClosures()} is the cached, file-backed entry point used by
     * other generators at runtime.
     *
     * @param siriData the raw IDFM disruption API response (must contain "disruptions" and "lines")
     * @return the closures described by the data, possibly empty
     */
    public List<StopClosure> computeStopClosures(JsonNode siriData) {
        List<StopClosure> closures = new ArrayList<>();
        if (siriData == null) {
            return closures;
        }

        Map<String, Object> alertDict = parseDisruptions(siriData.get("disruptions"));
        Map<String, Object> lines = parseLines(siriData.get("lines"));

        for (Map.Entry<String, Object> entry : alertDict.entrySet()) {
            String disruptionId = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> alert = (Map<String, Object>) entry.getValue();

            String severity = (String) alert.get(FIELD_SEVERITY);
            if (mapEffect(severity) != GtfsRealtime.Alert.Effect.NO_SERVICE) {
                // Only a genuine "no service" disruption means the stop is actually skipped;
                // e.g. a REDUCED_SERVICE disruption doesn't mean the stop stops being served.
                continue;
            }

            List<GtfsRealtime.TimeRange.Builder> periods = new ArrayList<>();
            for (JsonNode applicationPeriod : (ArrayNode) alert.get(FIELD_APPLICATION_PERIODS)) {
                periods.add(createTimeRange(applicationPeriod));
            }
            if (periods.isEmpty()) {
                continue;
            }
            List<StopClosure.Window> windows = new ArrayList<>();
            for (GtfsRealtime.TimeRange.Builder period : periods) {
                windows.add(new StopClosure.Window(period.getStart(), period.getEnd()));
            }

            ArrayNode impactedSections = (ArrayNode) alert.get(FIELD_IMPACTED_SECTIONS);
            for (RouteImpact impact : computeRouteImpacts(disruptionId, lines, impactedSections)) {
                if (!impact.sections().isEmpty()) {
                    // Sections are the more reliable source when present — see
                    // computeRouteImpacts's javadoc for why the per-stop list can't be trusted
                    // on its own here.
                    closures.add(new StopClosure(disruptionId, impact.routeId(), List.of(), impact.sections(), windows, false));
                } else if (!impact.stopIds().isEmpty()) {
                    closures.add(new StopClosure(disruptionId, impact.routeId(), impact.stopIds(), List.of(), windows, false));
                } else if (impact.lineImpacted()) {
                    // No specific stops or sections named at all: IDFM is describing a genuine
                    // whole-line closure (e.g. maintenance work), so every trip on the route is
                    // affected rather than just some stops on it.
                    closures.add(new StopClosure(disruptionId, impact.routeId(), List.of(), List.of(), windows, true));
                }
            }
        }

        return closures;
    }

    /**
     * Returns the stop-level closures from the most recently fetched disruption data, cached on
     * disk by {@link #generateAlert()} (see {@code alerts_data.json}).
     * <p>
     * Reads the cached file rather than calling {@link AlertFetcher} again, since this is meant
     * to be polled frequently (e.g. every TripUpdates generation cycle) independently of the
     * alerts generation schedule.
     *
     * @return the currently active stop-level closures, or an empty list if no disruption data
     *         has been fetched yet or it could not be read
     */
    public List<StopClosure> getActiveStopClosures() {
        java.io.File file = new java.io.File("alerts_data.json");
        if (!file.exists()) {
            return new ArrayList<>();
        }

        try {
            JsonNode siriData = new ObjectMapper().readTree(file);
            return computeStopClosures(siriData);
        } catch (java.io.IOException e) {
            System.err.println("Error reading cached alerts data from " + file + ": " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Converts a date-time string to Unix epoch time (seconds since January 1, 1970).
     * 
     * <p>The input string must be in the format "yyyyMMdd'T'HHmmss" (e.g., "20231225T143000").
     * The conversion is performed using the Europe/Paris timezone to match IDFM's local time.</p>
     * 
     * @param dateTimeStr the date-time string to convert, formatted as "yyyyMMdd'T'HHmmss"
     * @return the Unix epoch timestamp in seconds
     * @throws java.time.format.DateTimeParseException if the date-time string cannot be parsed
     */
    private long convertToEpoch(String dateTimeStr) {
        // Update the formatter to match the date string format
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
        java.time.LocalDateTime dateTime = java.time.LocalDateTime.parse(dateTimeStr, formatter);
        java.time.ZoneId zoneId = java.time.ZoneId.of("Europe/Paris");
        return dateTime.atZone(zoneId).toEpochSecond();
    }

    /**
     * Parses disruption data from the IDFM API response into a structured map.
     * 
     * <p>Each disruption is converted into a map containing the following fields:</p>
     * <ul>
     *   <li><b>id</b>: Unique identifier for the disruption</li>
     *   <li><b>applicationPeriods</b>: Array of time periods when the disruption is active</li>
     *   <li><b>lastUpdate</b>: Timestamp of the last update to this disruption</li>
     *   <li><b>cause</b>: Cause of the disruption (e.g., "TRAVAUX", "PERTURBATION")</li>
     *   <li><b>severity</b>: Severity level (e.g., "BLOQUANTE", "PERTURBEE")</li>
     *   <li><b>tags</b>: Additional classification tags</li>
     *   <li><b>title</b>: Short title/headline for the alert</li>
     *   <li><b>message</b>: Detailed description of the disruption</li>
     * </ul>
     * 
     * <p>Disruptions missing mandatory fields (id or applicationPeriods) are skipped
     * and a warning is logged to stderr.</p>
     * 
     * @param disruptions JSON array node containing disruption objects from the IDFM API
     * @return a map where keys are disruption IDs and values are maps containing disruption details
     */
    public Map<String, Object> parseDisruptions(JsonNode disruptions) {
        Map<String, Object> alertDict = new HashMap<>();
    
        for (JsonNode disruption : disruptions) {
            String id = getStringField(disruption, "id");
            ArrayNode applicationPeriods = getArrayNodeField(disruption, FIELD_APPLICATION_PERIODS);
    
            if (!isValidDisruption(id, applicationPeriods)) {
                continue;
            }
    
            Map<String, Object> alert = createAlertFromDisruption(disruption, id, applicationPeriods);
            alertDict.put(id, alert);
        }
    
        return alertDict;
    }

    /**
     * Extracts a string field from a JSON node, returning null if not present.
     *
     * @param node the JSON node to extract from
     * @param fieldName the name of the field to extract
     * @return the field value as a string, or null if not present
     */
    private String getStringField(JsonNode node, String fieldName) {
        return node.has(fieldName) ? node.get(fieldName).asText() : null;
    }

    /**
     * Extracts an ArrayNode field from a JSON node, returning null if not present.
     *
     * @param node the JSON node to extract from
     * @param fieldName the name of the field to extract
     * @return the field value as an ArrayNode, or null if not present
     */
    private ArrayNode getArrayNodeField(JsonNode node, String fieldName) {
        return node.has(fieldName) ? (ArrayNode) node.get(fieldName) : null;
    }

    /**
     * Validates that a disruption has all mandatory fields.
     *
     * @param id the disruption ID
     * @param applicationPeriods the application periods
     * @return true if valid, false otherwise
     */
    private boolean isValidDisruption(String id, ArrayNode applicationPeriods) {
        if (id == null || applicationPeriods == null) {
            System.err.println("Skipping disruption due to missing mandatory fields: id or applicationPeriods");
            return false;
        }
        return true;
    }

    /**
     * Creates an alert map from a disruption JSON node.
     *
     * @param disruption the disruption JSON node
     * @param id the disruption ID
     * @param applicationPeriods the application periods
     * @return a map containing all alert fields
     */
    private Map<String, Object> createAlertFromDisruption(JsonNode disruption, String id, ArrayNode applicationPeriods) {
        Map<String, Object> alert = new HashMap<>();
        alert.put("id", id);
        alert.put(FIELD_APPLICATION_PERIODS, applicationPeriods);
        alert.put(FIELD_LAST_UPDATE, getStringField(disruption, FIELD_LAST_UPDATE));
        alert.put(FIELD_CAUSE, getStringField(disruption, FIELD_CAUSE));
        alert.put(FIELD_SEVERITY, getStringField(disruption, FIELD_SEVERITY));
        alert.put("tags", getArrayNodeField(disruption, "tags"));
        alert.put(FIELD_TITLE, getStringField(disruption, FIELD_TITLE));
        alert.put(FIELD_MESSAGE, getStringField(disruption, FIELD_MESSAGE));
        alert.put(FIELD_IMPACTED_SECTIONS, getArrayNodeField(disruption, FIELD_IMPACTED_SECTIONS));
        return alert;
    }

    /**
     * Parses transit line data from the IDFM API response into a structured map.
     * 
     * <p>Each line is converted into a map containing the following fields:</p>
     * <ul>
     *   <li><b>id</b>: Unique identifier for the transit line</li>
     *   <li><b>name</b>: Full name of the line</li>
     *   <li><b>shortName</b>: Short name or number of the line (e.g., "1", "A", "RER A")</li>
     *   <li><b>mode</b>: Transit mode (e.g., "metro", "bus", "rer", "tramway")</li>
     *   <li><b>networkId</b>: Identifier of the network this line belongs to</li>
     *   <li><b>impactedObjects</b>: Array of objects (stops, routes, etc.) impacted by disruptions on this line</li>
     * </ul>
     * 
     * <p>The impactedObjects array contains references to disruption IDs, allowing the generator
     * to create informed entity selectors that specify which routes and stops are affected by each alert.</p>
     * 
     * @param lines JSON array node containing line objects from the IDFM API
     * @return a map where keys are line IDs and values are maps containing line details and impacted objects
     */
    public Map<String, Object> parseLines(JsonNode lines) {
        Map<String, Object> linesDict = new HashMap<>();

        for (JsonNode line : lines) {
            String id = line.get("id").asText();
            String name = line.get("name").asText();
            String shortName = line.get("shortName").asText();
            String mode = line.get("mode").asText();
            String networkId = line.get("networkId").asText();
            ArrayNode impactedObjects = (ArrayNode) line.get(FIELD_IMPACTED_OBJECTS);

            Map<String, Object> lineDict = new HashMap<>();
            lineDict.put("id", id);
            lineDict.put("name", name);
            lineDict.put("shortName", shortName);
            lineDict.put("mode", mode);
            lineDict.put("networkId", networkId);
            lineDict.put(FIELD_IMPACTED_OBJECTS, impactedObjects);

            linesDict.put(id, lineDict);
        }

        return linesDict;
    }
}