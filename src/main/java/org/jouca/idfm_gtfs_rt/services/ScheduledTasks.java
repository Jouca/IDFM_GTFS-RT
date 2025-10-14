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

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Service responsible for scheduling and executing periodic tasks related to GTFS and GTFS-RT data generation.
 * <p>
 * This service manages two main scheduled operations:
 * <ul>
 *   <li>Alert generation: Runs every 10 seconds to fetch alerts and generate GTFS-RT alert feeds</li>
 *   <li>Trip updates generation: Runs every 2 minutes to fetch real-time trip updates and generate GTFS-RT trip update feeds</li>
 * </ul>
 * <p>
 * The service also handles GTFS static data updates by checking if the local database is outdated (older than 24 hours)
 * and fetching new data when necessary.
 * <p>
 * Thread-safety is ensured through the use of reentrant locks to prevent concurrent execution of the same task.
 * 
 * @author Jouca
 * @since 1.0
 *
 * @see TripUpdateGenerator
 * @see AlertGenerator
 * @see GTFSFetcher
 */
@Service
public class ScheduledTasks {
    
    /**
     * Generator for GTFS-RT trip updates based on real-time data.
     */
    @Autowired
    private TripUpdateGenerator gtfsrtGenerator;

    /**
     * Generator for GTFS-RT service alerts.
     */
    @Autowired
    private AlertGenerator alertGenerator;

    /**
     * Environment configuration loader for accessing environment variables and configuration settings.
     */
    private static final Dotenv dotenv = Dotenv.configure().directory("/app").load();

    /**
     * Lock to prevent concurrent execution of alert update tasks.
     */
    private final ReentrantLock lockAlertUpdate = new ReentrantLock();
    
    /**
     * Lock to prevent concurrent execution of trip update tasks.
     */
    private final ReentrantLock lockTripUpdate = new ReentrantLock();

    /**
     * File path to the local SQLite database containing GTFS static data.
     */
    private static final String GTFS_FILE_PATH = "./gtfs.db";
    
    /**
     * URL to download GTFS static data from. Can be configured via the GTFS_URL environment variable.
     * Defaults to the Île-de-France Mobilités GTFS data URL if not specified.
     */
    private static final String GTFS_URL = (dotenv.get("GTFS_URL") != null && !dotenv.get("GTFS_URL").isEmpty())
        ? dotenv.get("GTFS_URL")
        : "https://data.iledefrance-mobilites.fr/explore/dataset/offre-horaires-tc-gtfs-idfm/files/a925e164271e4bca93433756d6a340d1/download/";

    /**
     * Checks if the GTFS static data database exists and is up-to-date.
     * <p>
     * This method performs the following checks:
     * <ol>
     *   <li>Verifies if the database file exists at the specified path</li>
     *   <li>If not found, fetches new GTFS data from the configured URL</li>
     *   <li>If found, checks the last modification time</li>
     *   <li>If the database is older than 24 hours, fetches fresh GTFS data</li>
     * </ol>
     * <p>
     * This ensures the application always works with recent static transit data,
     * which is crucial for accurate trip matching and schedule information.
     *
     * @throws Exception if an error occurs while checking file attributes or fetching data
     */
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

    /**
     * Scheduled task that fetches service alerts and generates GTFS-RT alert feeds.
     * <p>
     * This method is executed every 10 seconds according to the cron schedule.
     * It uses a reentrant lock to prevent concurrent executions. If a previous
     * execution is still in progress, the new execution is skipped.
     * <p>
     * The method delegates the actual alert generation to the {@link AlertGenerator}.
     * Any exceptions during the generation process are caught and logged to prevent
     * disruption of the scheduled task execution.
     * <p>
     * <strong>Schedule:</strong> Every 10 seconds
     *
     * @see AlertGenerator#generateAlert()
     */
    @Scheduled(cron = "*/10 * * * * ?") // Every 10 seconds
    public void fetchAlertsAndGenerateGTFSRT() {
        if (lockAlertUpdate.tryLock()) {
            try {
                System.out.println("[Alerts] Generating GTFS-RT...");
                alertGenerator.generateAlert();
                System.out.println("[Alerts] GTFS-RT generated !");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lockAlertUpdate.unlock();
            }
        } else {
            System.out.println("[Alerts] GTFS download in progress, skipping GTFS-RT generation.");
        }
    }

    /**
     * Scheduled task that fetches trip updates and generates GTFS-RT trip update feeds.
     * <p>
     * This method is executed every 2 minutes according to the cron schedule.
     * It uses a reentrant lock to prevent concurrent executions. If a previous
     * execution is still in progress, the new execution is skipped.
     * <p>
     * Before generating trip updates, this method checks if the GTFS static database
     * exists. If the database is not found, the generation is skipped to avoid errors.
     * <p>
     * The method delegates the actual trip update generation to the {@link TripUpdateGenerator}.
     * Any exceptions during the generation process are caught and logged to prevent
     * disruption of the scheduled task execution.
     * <p>
     * <strong>Schedule:</strong> Every 2 minutes
     *
     * @see TripUpdateGenerator#generateGTFSRT()
     */
    @Scheduled(cron = "0 */2 * * * ?") // Every 2 minutes
    public void fetchTripUpdatesAndGenerateGTFSRT() {
        if (lockTripUpdate.tryLock()) {
            try {
                // Check if the SQLite is here
                if (!Files.exists(Path.of(GTFS_FILE_PATH))) {
                    System.out.println("SQLite database not found. Skipping Trips generation.");
                }

                System.out.println("[Trips] Generating GTFS-RT...");
                gtfsrtGenerator.generateGTFSRT();
                System.out.println("[Trips] GTFS-RT generated !");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lockTripUpdate.unlock();
            }
        } else {
            System.out.println("[Trips] GTFS download in progress, skipping GTFS-RT generation.");
        }
    }
}