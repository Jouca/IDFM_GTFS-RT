package org.jouca.idfm_gtfs_rt;

import org.jouca.idfm_gtfs_rt.services.ScheduledTasks;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GTFSRTApplication implements ApplicationListener<ContextRefreshedEvent> {
    private final ScheduledTasks scheduledTasks = new ScheduledTasks();

    public static void main(String[] args) {
        SpringApplication.run(GTFSRTApplication.class, args);
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        scheduledTasks.checkAndUpdateGTFSData();
    }
}