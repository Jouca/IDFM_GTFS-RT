package org.jouca.idfm_gtfs_rt.services;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.jouca.idfm_gtfs_rt.fetchers.GTFSFetcher;
import org.jouca.idfm_gtfs_rt.generator.AlertGenerator;
import org.jouca.idfm_gtfs_rt.generator.TripUpdateGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ScheduledTasks {
    @Autowired
    private TripUpdateGenerator gtfsrtGenerator;

    @Autowired
    private AlertGenerator alertGenerator;

    private final ReentrantLock lockAlertUpdate = new ReentrantLock();
    private final ReentrantLock lockTripUpdate = new ReentrantLock();

    private static final String GTFS_FILE_PATH = "./gtfs.db";
    private static final String GTFS_URL = "https://clarifygdps.com/bridge/gtfs/fr-idf.zip";

    public void checkAndUpdateGTFSData() {
        try {
            Path dbPath = Path.of(GTFS_FILE_PATH);

            // Check if the database file exists
            if (!Files.exists(dbPath)) {
                System.out.println("SQLite database not found. Fetching GTFS data...");
                GTFSFetcher.fetchGTFS(GTFS_URL, GTFS_FILE_PATH);
                return;
            }

            // Check if the database was updated within the last 24 hours
            BasicFileAttributes attrs = Files.readAttributes(dbPath, BasicFileAttributes.class);
            Instant lastModifiedTime = attrs.lastModifiedTime().toInstant();
            Instant now = Instant.now();

            if (ChronoUnit.HOURS.between(lastModifiedTime, now) > 24) {
                System.out.println("SQLite database is outdated. Fetching GTFS data...");
                GTFSFetcher.fetchGTFS(GTFS_URL, GTFS_FILE_PATH);
            } else {
                System.out.println("SQLite database is up-to-date.");
            }
        } catch (Exception e) {
            System.err.println("Failed to check or update GTFS data:");
            e.printStackTrace();
        }
    }

    @Scheduled(cron = "*/10 * * * * ?") // Every 10 seconds
    public void fetchAlertsAndGenerateGTFSRT() {
        if (lockAlertUpdate.tryLock()) {
            try {
                System.out.println("[Alerts] Génération du GTFS-RT en cours...");
                lockAlertUpdate.lock();

                alertGenerator.generateAlert();
                System.out.println("[Alerts] GTFS-RT généré avec succès !");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lockAlertUpdate.unlock();
            }
        } else {
            System.out.println("[Alerts] GTFS download in progress, skipping GTFS-RT generation.");
        }
    }

    @Scheduled(cron = "0 */2 * * * ?") // Every 2 minutes
    public void fetchTripUpdatesAndGenerateGTFSRT() {
        if (lockTripUpdate.tryLock()) {
            try {
                // Check if the SQLite is here
                if (!Files.exists(Path.of(GTFS_FILE_PATH))) {
                    System.out.println("SQLite database not found. Skipping Trips generation.");
                }

                System.out.println("[Trips] Génération du GTFS-RT en cours...");
                lockTripUpdate.lock();

                gtfsrtGenerator.generateGTFSRT();
                System.out.println("[Trips] GTFS-RT généré avec succès !");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lockTripUpdate.unlock();
            }
        } else {
            System.out.println("[Trips] GTFS download in progress, skipping GTFS-RT generation.");
        }
    }

    /*@Scheduled(cron = "0 0 0 * * ?") // Every day at midnight
    public void updateGTFSData() {
        System.out.println("Updating GTFS data...");
        try {
            GTFSFetcher.fetchGTFS(GTFS_URL, GTFS_FILE_PATH);
            
            System.out.println("GTFS data updated successfully!");
        } catch (Exception e) {
            System.err.println("Failed to update GTFS data:");
            e.printStackTrace();
        }
    }*/
}