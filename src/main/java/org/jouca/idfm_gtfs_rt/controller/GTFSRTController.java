package org.jouca.idfm_gtfs_rt.controller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

/**
 * REST controller for serving GTFS-RT (General Transit Feed Specification - Realtime) data.
 * 
 * <p>This controller provides endpoints for accessing real-time transit information
 * including alerts, trip updates, and SIRI-Lite data for the IDFM (Île-de-France Mobilités)
 * transit network.</p>
 * 
 * <p>The controller serves pre-generated Protocol Buffer (.pb) files for GTFS-RT data
 * and JSON files for SIRI-Lite data, which are typically updated by scheduled tasks.</p>
 * 
 * @author Jouca
 * @since 1.0
 */
@RestController
public class GTFSRTController {
    
    /**
     * Retrieves the GTFS-RT alerts feed file for IDFM.
     * 
     * <p>This endpoint serves the Protocol Buffer file containing real-time service alerts
     * for the IDFM transit network. The file includes information about service disruptions,
     * delays, and other important transit alerts.</p>
     * 
     * <p>The response is served as an octet-stream with appropriate headers for file download.</p>
     * 
     * @return ResponseEntity containing the binary content of the GTFS-RT alerts file with
     *         HTTP status 200 (OK), or HTTP status 404 (NOT_FOUND) if the file doesn't exist
     * @throws Exception if there's an error reading the file from the filesystem
     */
    @GetMapping("/gtfs-rt-alerts-idfm")
    public ResponseEntity<byte[]> getGtfsRtAlertFile() throws Exception {
        Path filePath = Paths.get("gtfs-rt-alerts-idfm.pb");
        
        // Check if the file exists
        if (!Files.exists(filePath)) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        byte[] fileContent = Files.readAllBytes(filePath);

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=gtfs-rt-idfm.pb");

        return new ResponseEntity<>(fileContent, headers, HttpStatus.OK);
    }

    /**
     * Retrieves the GTFS-RT trip updates feed file for IDFM.
     * 
     * <p>This endpoint serves the Protocol Buffer file containing real-time trip updates
     * for the IDFM transit network. The file includes information about vehicle positions,
     * estimated arrival and departure times, and schedule adherence.</p>
     * 
     * <p>The response is served as an octet-stream with appropriate headers for file download.</p>
     * 
     * @return ResponseEntity containing the binary content of the GTFS-RT trip updates file with
     *         HTTP status 200 (OK), or HTTP status 404 (NOT_FOUND) if the file doesn't exist
     * @throws Exception if there's an error reading the file from the filesystem
     */
    @GetMapping("/gtfs-rt-trips-idfm")
    public ResponseEntity<byte[]> getGtfsRtTripFile() throws Exception {
        Path filePath = Paths.get("gtfs-rt-trips-idfm.pb");
        
        // Check if the file exists
        if (!Files.exists(filePath)) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        byte[] fileContent = Files.readAllBytes(filePath);

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=gtfs-rt-idfm.pb");

        return new ResponseEntity<>(fileContent, headers, HttpStatus.OK);
    }

    /**
     * Retrieves specific trip entities by their trip IDs.
     * 
     * <p>This endpoint accepts a comma-separated list of trip IDs and returns the corresponding
     * entity information from the cached entities file. Each trip ID is looked up in the
     * entities_trips.json file, and matching entities are returned in a JSON response.</p>
     * 
     * <p>The trip IDs are parsed from a comma-separated string, with whitespace automatically
     * trimmed from each ID. If a trip ID is not found in the cache, it is simply omitted from
     * the response.</p>
     * 
     * <p><b>Example usage:</b></p>
     * <pre>
     * POST /getEntities?tripIds=trip1,trip2,trip3
     * </pre>
     * 
     * @param tripIdsParam comma-separated string of trip IDs to retrieve (e.g., "trip1,trip2,trip3")
     * @return ResponseEntity containing a JSON object mapping trip IDs to their entity data with
     *         HTTP status 200 (OK), or HTTP status 404 (NOT_FOUND) if the entities file is missing
     *         or empty, or HTTP status 500 (INTERNAL_SERVER_ERROR) if there's an error reading
     *         or parsing the data
     */
    @PostMapping("/getEntities")
    public ResponseEntity<String> getEntity(@RequestParam("tripIds") String tripIdsParam) {
        HttpHeaders headers = new HttpHeaders();

        // Split the comma-separated string into a list, trimming whitespace
        List<String> tripIds = new ArrayList<>();
        if (tripIdsParam != null && !tripIdsParam.isEmpty()) {
            for (String id : tripIdsParam.split(",")) {
                tripIds.add(id.trim());
            }
        }

        Path filePath = Paths.get("entities_trips.json");
        String json;
        try {
            json = new String(Files.readAllBytes(filePath));
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        if (json == null || json.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        JsonNode siriLiteData;
        try {
            siriLiteData = new ObjectMapper().readTree(json);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        HashMap<String, JsonNode> entities = new HashMap<>();
        for (String tripId : tripIds) {
            JsonNode entity = siriLiteData.get(tripId);
            if (entity != null) {
                entities.put(tripId, entity);
            }
        }

        headers.add(HttpHeaders.CONTENT_TYPE, "application/json");
        try {
            String responseJson = new ObjectMapper().writeValueAsString(entities);
            return new ResponseEntity<>(responseJson, headers, HttpStatus.OK);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Retrieves the SIRI-Lite data file with pretty-printed JSON formatting.
     * 
     * <p>This endpoint serves the SIRI-Lite (Service Interface for Real-time Information - Lite)
     * data file, which contains real-time transit information in a standardized European format.
     * The JSON data is automatically formatted with 4-space indentation for better readability.</p>
     * 
     * <p>The SIRI-Lite format is commonly used in European transit systems and provides
     * real-time information about vehicle positions, estimated times, and service status.</p>
     * 
     * <p>The response includes appropriate headers for JSON content and file download.</p>
     * 
     * <p><b>Note:</b> This endpoint is only available when the application is running
     * in the "debug" Spring profile.</p>
     * 
     * @return ResponseEntity containing the formatted JSON content of the SIRI-Lite data file with
     *         HTTP status 200 (OK), or HTTP status 404 (NOT_FOUND) if the file doesn't exist or
     *         is empty, or HTTP status 500 (INTERNAL_SERVER_ERROR) if there's an error reading
     *         or formatting the data
     * @throws Exception if there's an error during file operations or JSON processing
     */
    @org.springframework.context.annotation.Profile("debug")
    @GetMapping("/siri-lite")
    public ResponseEntity<byte[]> getSiriLiteFile() throws Exception {
        Path filePath = Paths.get("sirilite_data.json");
        // Check if the file exists
        if (!Files.exists(filePath)) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        try {
            String rawJson = Files.readString(filePath);
            if (rawJson == null || rawJson.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(rawJson);

            // Configure pretty printer with 4-space indentation for both objects & arrays
            DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
            DefaultIndenter indenter = new DefaultIndenter("    ", DefaultIndenter.SYS_LF);
            pp.indentObjectsWith(indenter);
            pp.indentArraysWith(indenter);

            byte[] formatted = mapper.writer(pp).writeValueAsBytes(root);

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=sirilite_data.json");

            return new ResponseEntity<>(formatted, headers, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}