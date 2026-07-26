package org.jouca.idfm_gtfs_rt.services;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantLock;

import org.jouca.idfm_gtfs_rt.fetchers.GTFSFetcher;
import org.jouca.idfm_gtfs_rt.finders.TripFinder;
import org.jouca.idfm_gtfs_rt.generator.AlertGenerator;
import org.jouca.idfm_gtfs_rt.generator.TripUpdateGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 *   <li>Trip updates generation: Runs every minute to fetch real-time trip updates and generate GTFS-RT trip update feeds</li>
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
    
    private static final Logger logger = LoggerFactory.getLogger(ScheduledTasks.class);
    
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
     * Configured to not fail if the .env file is missing (for test environments).
     */
    private static final Dotenv dotenv = Dotenv.configure()
            .directory("/app")
            .ignoreIfMissing()
            .load();

    /**
     * Lock to prevent concurrent execution of alert update tasks.
     */
    private final ReentrantLock lockAlertUpdate = new ReentrantLock();
    
    /**
     * Lock to prevent concurrent execution of trip update tasks.
     */
    private final ReentrantLock lockTripUpdate = new ReentrantLock();

    /**
     * Lock to prevent concurrent GTFS refresh runs (e.g. if a previous refresh is still in progress).
     */
    private final ReentrantLock lockGtfsRefresh = new ReentrantLock();

    /**
     * File path to the active SQLite database containing GTFS static data.
     */
    private static final String GTFS_FILE_PATH = "./gtfs-data/gtfs.db";

    /**
     * Temporary file path used while building a new GTFS database before hot-swapping it in.
     */
    private static final String GTFS_TEMP_FILE_PATH = "./gtfs-data/gtfs.db.new";
    
    /**
     * Environment variable key for the GTFS URL configuration.
     */
    private static final String GTFS_URL_ENV_KEY = "GTFS_URL";
    
    /**
     * URL to download GTFS static data from. Can be configured via the GTFS_URL environment variable.
     * Defaults to the Île-de-France Mobilités GTFS data URL if not specified.
     */
    private static final String GTFS_URL = (dotenv.get(GTFS_URL_ENV_KEY) != null && !dotenv.get(GTFS_URL_ENV_KEY).isEmpty())
        ? dotenv.get(GTFS_URL_ENV_KEY)
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

            if (!Files.exists(dbPath)) {
                // No DB at all: download synchronously so the app can serve requests.
                logger.info("SQLite database not found at {}. Fetching GTFS data from {}...", GTFS_FILE_PATH, GTFS_URL);
                GTFSFetcher.fetchGTFS(GTFS_URL, GTFS_FILE_PATH);
                logger.info("GTFS data fetch completed successfully.");
            } else {
                // DB exists from a previous run: refresh in the background so the app
                // starts immediately and serves the old data while the new one is built.
                logger.info("SQLite database found. Triggering background GTFS refresh...");
                new Thread(this::refreshGTFSData, "gtfs-startup-refresh").start();
            }
        } catch (Exception e) {
            logger.error("Failed to check or update GTFS data: {}", e.getMessage(), e);
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
                logger.debug("Error generating alerts GTFS-RT", e);
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
     * This method is executed every minute according to the cron schedule.
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
     * <strong>Schedule:</strong> Every minute
     *
     * @see TripUpdateGenerator#generateGTFSRT()
     */
    @Scheduled(cron = "0 * * * * ?") // Every minute
    public void fetchTripUpdatesAndGenerateGTFSRT() {
        if (lockTripUpdate.tryLock()) {
            try {
                // Check if the SQLite is here
                if (!Files.exists(Path.of(GTFS_FILE_PATH))) {
                    System.out.println("SQLite database not found. Skipping Trips generation.");
                    return;
                }

                System.out.println("[Trips] Generating GTFS-RT...");
                gtfsrtGenerator.generateGTFSRT();
                System.out.println("[Trips] GTFS-RT generated !");
            } catch (Exception e) {
                logger.debug("Error generating trip updates GTFS-RT", e);
            } finally {
                lockTripUpdate.unlock();
            }
        } else {
            System.out.println("[Trips] GTFS download in progress, skipping GTFS-RT generation.");
        }
    }

    /**
     * Refreshes the GTFS static database in-place without interrupting the GTFS-RT service.
     * <p>
     * Runs at 3:00 AM, 8:00 AM, 1:00 PM and 5:00 PM to match the IDFM publication schedule.
     * The new database is built into a temporary file first; only once the build is complete
     * is the active database replaced atomically and the connection pool reloaded.
     * At most one minute of trip-update generation is skipped during the swap itself.
     * <p>
     * <strong>Schedule:</strong> Daily at 03:00, 08:00, 13:00 and 17:00
     */
    @Scheduled(cron = "0 0 3,8,13,17 * * ?")
    public void refreshGTFSData() {
        if (!lockGtfsRefresh.tryLock()) {
            logger.info("[GTFS] Refresh already in progress, skipping.");
            return;
        }
        try {
            logger.info("[GTFS] Starting scheduled GTFS refresh (building into {})...", GTFS_TEMP_FILE_PATH);
            GTFSFetcher.fetchGTFS(GTFS_URL, GTFS_TEMP_FILE_PATH);

            // Hold the trip-update lock only for the instant of the atomic swap so that
            // no concurrent generation can read a half-replaced database.
            lockTripUpdate.lock();
            try {
                Files.move(Path.of(GTFS_TEMP_FILE_PATH), Path.of(GTFS_FILE_PATH),
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                TripFinder.reloadDataSource();
                logger.info("[GTFS] Database hot-swapped successfully.");
            } finally {
                lockTripUpdate.unlock();
            }
        } catch (Exception e) {
            logger.error("[GTFS] Refresh failed: {}", e.getMessage(), e);
        } finally {
            lockGtfsRefresh.unlock();
        }
    }
}