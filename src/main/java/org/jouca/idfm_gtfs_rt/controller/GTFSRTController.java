package org.jouca.idfm_gtfs_rt.controller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

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

    @GetMapping("/siri-lite")
    public ResponseEntity<byte[]> getSiriLiteFile() throws Exception {
        Path filePath = Paths.get("sirilite_data.json");
        
        // Check if the file exists
        if (!Files.exists(filePath)) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        byte[] fileContent = Files.readAllBytes(filePath);

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, "application/json");
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=sirilite_data.json");

        return new ResponseEntity<>(fileContent, headers, HttpStatus.OK);
    }
}