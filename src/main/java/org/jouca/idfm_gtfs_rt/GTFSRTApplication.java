package org.jouca.idfm_gtfs_rt;

import org.jouca.idfm_gtfs_rt.services.ScheduledTasks;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main Spring Boot application class for the GTFS-RT (General Transit Feed Specification - Real Time) service.
 * <p>
 * This application provides real-time transit information for the IDFM (Île-de-France Mobilités) network.
 * It listens for application startup events and triggers initial data synchronization.
 * </p>
 * 
 * <p>
 * Key features:
 * <ul>
 *   <li>Enables Spring Boot auto-configuration</li>
 *   <li>Enables scheduled task execution</li>
 *   <li>Listens for application context refresh events</li>
 *   <li>Triggers GTFS data update on application startup</li>
 * </ul>
 * </p>
 * 
 * @author Jouca
 * @since 1.0
 * 
 * @see ScheduledTasks
 * @see ApplicationListener
 */
@SpringBootApplication
@EnableScheduling
public class GTFSRTApplication implements ApplicationListener<ContextRefreshedEvent> {
    
    /**
     * Instance of {@link ScheduledTasks} responsible for managing periodic GTFS data updates.
     */
    private final ScheduledTasks scheduledTasks = new ScheduledTasks();

    /**
     * Main entry point for the Spring Boot application.
     * <p>
     * This method bootstraps the application by initializing the Spring application context,
     * loading all configurations, and starting the embedded web server.
     * </p>
     * 
     * @param args command-line arguments passed to the application
     */
    public static void main(String[] args) {
        SpringApplication.run(GTFSRTApplication.class, args);
    }

    /**
     * Event handler triggered when the Spring application context is refreshed or initialized.
     * <p>
     * This method is called automatically by Spring when the application context is fully loaded
     * and ready. It triggers an immediate check and update of GTFS data to ensure the application
     * starts with the most recent transit information available.
     * </p>
     * 
     * @param event the {@link ContextRefreshedEvent} containing information about the application context
     * @see ScheduledTasks#checkAndUpdateGTFSData()
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        scheduledTasks.checkAndUpdateGTFSData();
    }
}