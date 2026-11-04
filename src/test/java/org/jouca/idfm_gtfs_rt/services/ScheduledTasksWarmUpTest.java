package org.jouca.idfm_gtfs_rt.services;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.jouca.idfm_gtfs_rt.generator.AlertGenerator;
import org.jouca.idfm_gtfs_rt.generator.TripUpdateGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class ScheduledTasksWarmUpTest {

    private ScheduledTasks tasks;
    private AlertGenerator alerts;
    private TripUpdateGenerator trips;

    @BeforeEach
    void setUp() {
        tasks = new ScheduledTasks();
        alerts = mock(AlertGenerator.class);
        trips = mock(TripUpdateGenerator.class);
        ReflectionTestUtils.setField(tasks, "alertGenerator", alerts);
        ReflectionTestUtils.setField(tasks, "gtfsrtGenerator", trips);
    }

    @Test
    void initialGenerationRunsTheAlertsThatTheTripUpdatesDependOn() throws Exception {
        tasks.generateFeedsOnce();
        verify(alerts).generateAlert();
    }

    @Test
    void aFailingAlertsFetchDoesNotBreakTheInitialGeneration() throws Exception {
        doThrow(new java.io.IOException("502")).when(alerts).generateAlert();
        assertDoesNotThrow(tasks::generateFeedsOnce);
    }

    @Test
    void warmUpRunsInTheBackgroundWithoutBlockingTheCaller() throws Exception {
        tasks.warmUpFeeds();
        verify(alerts, timeout(5000)).generateAlert();
    }
}
