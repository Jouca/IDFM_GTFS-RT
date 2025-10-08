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

@RestController
public class GTFSRTController {
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