package org.jouca.idfm_gtfs_rt.controller;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** The feeds are served with validators so that pollers only download them when they changed. */
class FeedCachingTest {

    private static final Path TRIPS = Paths.get("gtfs-rt-trips-idfm.pb");
    private static final Path ALERTS = Paths.get("gtfs-rt-alerts-idfm.pb");
    private MockMvc mvc;

    @BeforeEach
    void setUp() throws Exception {
        mvc = MockMvcBuilders.standaloneSetup(new GTFSRTController()).build();
        Files.write(TRIPS, "trips-v1".getBytes());
        Files.write(ALERTS, "alerts-v1".getBytes());
        Files.setLastModifiedTime(TRIPS, FileTime.from(Instant.parse("2026-11-04T10:00:00Z")));
    }

    @AfterEach
    void tearDown() throws Exception {
        Files.deleteIfExists(TRIPS);
        Files.deleteIfExists(ALERTS);
    }

    @Test
    void feedIsServedAsProtobufWithValidatorsAndMustRevalidate() throws Exception {
        MvcResult r = mvc.perform(MockMvcRequestBuilders.get("/gtfs-rt-trips-idfm")).andReturn();

        assertEquals(200, r.getResponse().getStatus());
        assertTrue(r.getResponse().getContentType().startsWith("application/x-protobuf"));
        assertTrue(r.getResponse().getHeader("ETag").startsWith("W/\""), "weak, so that gzip stays possible");
        assertNotNull(r.getResponse().getHeader("Last-Modified"));
        assertEquals("no-cache", r.getResponse().getHeader("Cache-Control"));
        assertFalse(r.getResponse().containsHeader("Content-Disposition"));
        assertArrayEquals("trips-v1".getBytes(), r.getResponse().getContentAsByteArray());
    }

    @Test
    void unchangedFeedAnswersNotModifiedWithoutBody() throws Exception {
        String etag = mvc.perform(MockMvcRequestBuilders.get("/gtfs-rt-trips-idfm")).andReturn()
                .getResponse().getHeader("ETag");

        MvcResult again = mvc.perform(MockMvcRequestBuilders.get("/gtfs-rt-trips-idfm")
                .header("If-None-Match", etag)).andReturn();

        assertEquals(304, again.getResponse().getStatus());
        assertEquals(0, again.getResponse().getContentAsByteArray().length);
    }

    @Test
    void changedFeedIsServedAgainWithANewValidator() throws Exception {
        String etag = mvc.perform(MockMvcRequestBuilders.get("/gtfs-rt-trips-idfm")).andReturn()
                .getResponse().getHeader("ETag");

        Files.write(TRIPS, "trips-v2-longer".getBytes());
        Files.setLastModifiedTime(TRIPS, FileTime.from(Instant.parse("2026-11-04T10:01:00Z")));
        MvcResult changed = mvc.perform(MockMvcRequestBuilders.get("/gtfs-rt-trips-idfm")
                .header("If-None-Match", etag)).andReturn();

        assertEquals(200, changed.getResponse().getStatus());
        assertNotEquals(etag, changed.getResponse().getHeader("ETag"));
        assertArrayEquals("trips-v2-longer".getBytes(), changed.getResponse().getContentAsByteArray());
    }

    @Test
    void alertsFeedBehavesTheSameAndMissingFeedIs404() throws Exception {
        MvcResult alerts = mvc.perform(MockMvcRequestBuilders.get("/gtfs-rt-alerts-idfm")).andReturn();
        assertEquals(200, alerts.getResponse().getStatus());
        assertNotNull(alerts.getResponse().getHeader("ETag"));

        Files.delete(ALERTS);
        assertEquals(404, mvc.perform(MockMvcRequestBuilders.get("/gtfs-rt-alerts-idfm")).andReturn()
                .getResponse().getStatus());
    }
}
