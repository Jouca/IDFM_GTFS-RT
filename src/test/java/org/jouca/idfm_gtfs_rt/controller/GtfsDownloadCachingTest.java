package org.jouca.idfm_gtfs_rt.controller;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * The static GTFS download exposes {@code Last-Modified} and an {@code ETag}, so consumers can tell whether the
 * archive changed without downloading it (a {@code HEAD}, or a conditional {@code GET}).
 */
class GtfsDownloadCachingTest {

    private static final byte[] ZIP = "PK-fake-gtfs-archive".getBytes();
    @TempDir
    Path dir;
    private Path zip;
    private MockMvc mvc;

    @BeforeEach
    void setUp() throws Exception {
        zip = dir.resolve("IDFM-gtfs-enriched.zip");
        Files.write(zip, ZIP);
        Files.setLastModifiedTime(zip, FileTime.from(Instant.parse("2026-11-16T08:03:00Z")));
        GTFSRTController controller = new GTFSRTController();
        controller.enrichedGtfsPath = zip;
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void getCarriesLastModifiedAndETagAndTheArchive() throws Exception {
        MvcResult r = mvc.perform(MockMvcRequestBuilders.get("/gtfs")).andReturn();

        assertEquals(200, r.getResponse().getStatus());
        assertEquals("application/zip", r.getResponse().getContentType());
        assertNotNull(r.getResponse().getHeader("ETag"));
        assertEquals("Mon, 16 Nov 2026 08:03:00 GMT", r.getResponse().getHeader("Last-Modified"));
        assertArrayEquals(ZIP, r.getResponse().getContentAsByteArray());
    }

    @Test
    void headGivesTheSameValidatorsAndTheLength() throws Exception {
        MvcResult get = mvc.perform(MockMvcRequestBuilders.get("/gtfs")).andReturn();
        MvcResult head = mvc.perform(MockMvcRequestBuilders.head("/gtfs")).andReturn();

        assertEquals(200, head.getResponse().getStatus());
        assertEquals(get.getResponse().getHeader("ETag"), head.getResponse().getHeader("ETag"));
        assertEquals(get.getResponse().getHeader("Last-Modified"), head.getResponse().getHeader("Last-Modified"));
        assertEquals(String.valueOf(ZIP.length), head.getResponse().getHeader("Content-Length"));
        // (MockMvc, unlike Tomcat, does not strip the body of a HEAD response, so it is not asserted here.)
    }

    @Test
    void unchangedArchiveAnswersNotModifiedToBothKindsOfConditionalRequest() throws Exception {
        MvcResult first = mvc.perform(MockMvcRequestBuilders.get("/gtfs")).andReturn();

        MvcResult byEtag = mvc.perform(MockMvcRequestBuilders.get("/gtfs")
                .header("If-None-Match", first.getResponse().getHeader("ETag"))).andReturn();
        MvcResult byDate = mvc.perform(MockMvcRequestBuilders.get("/gtfs")
                .header("If-Modified-Since", first.getResponse().getHeader("Last-Modified"))).andReturn();

        assertEquals(304, byEtag.getResponse().getStatus());
        assertEquals(304, byDate.getResponse().getStatus());
        assertEquals(0, byEtag.getResponse().getContentAsByteArray().length);
    }

    @Test
    void regeneratedArchiveIsServedAgainWithNewValidators() throws Exception {
        MvcResult first = mvc.perform(MockMvcRequestBuilders.get("/gtfs")).andReturn();
        String etag = first.getResponse().getHeader("ETag");

        Files.write(zip, "PK-a-newer-and-longer-archive".getBytes());
        Files.setLastModifiedTime(zip, FileTime.from(Instant.parse("2026-11-16T11:00:00Z")));
        MvcResult second = mvc.perform(MockMvcRequestBuilders.get("/gtfs")
                .header("If-None-Match", etag)
                .header("If-Modified-Since", first.getResponse().getHeader("Last-Modified"))).andReturn();

        assertEquals(200, second.getResponse().getStatus());
        assertNotEquals(etag, second.getResponse().getHeader("ETag"));
        assertEquals("Mon, 16 Nov 2026 11:00:00 GMT", second.getResponse().getHeader("Last-Modified"));
    }

    @Test
    void missingArchiveIs404() throws Exception {
        Files.delete(zip);
        assertEquals(404, mvc.perform(MockMvcRequestBuilders.get("/gtfs")).andReturn().getResponse().getStatus());
        assertEquals(404, mvc.perform(MockMvcRequestBuilders.head("/gtfs")).andReturn().getResponse().getStatus());
    }

    @Test
    void rangeRequestsAreSupportedSoALargeDownloadCanBeResumed() throws Exception {
        MvcResult r = mvc.perform(MockMvcRequestBuilders.get("/gtfs").header("Range", "bytes=0-1")).andReturn();
        assertEquals(206, r.getResponse().getStatus());
        assertArrayEquals("PK".getBytes(), r.getResponse().getContentAsByteArray());
        assertTrue(r.getResponse().getHeader("Content-Range").startsWith("bytes 0-1/"));
    }
}
