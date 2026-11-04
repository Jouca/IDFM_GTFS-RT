package org.jouca.idfm_gtfs_rt.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourceFingerprintTest {

    @TempDir
    Path dir;
    Path record;
    Path db;
    final Instant now = Instant.parse("2026-11-04T08:00:00Z");
    final Duration maxAge = Duration.ofHours(20);

    @BeforeEach
    void setUp() throws Exception {
        record = dir.resolve("gtfs-source.fingerprint");
        db = dir.resolve("gtfs.db");
        Files.writeString(db, "x");
    }

    private static String fp(String content) throws Exception {
        return SourceFingerprint.of(new ByteArrayInputStream(content.getBytes()));
    }

    private void recordedHoursAgo(String fingerprint, long hours) throws Exception {
        SourceFingerprint.record(record, fingerprint);
        Files.setLastModifiedTime(record, FileTime.from(now.minus(Duration.ofHours(hours))));
    }

    @Test
    void sameBytesGiveTheSameFingerprintAndOtherBytesAnotherOne() throws Exception {
        assertEquals(fp("zip-A"), fp("zip-A"));
        assertNotEquals(fp("zip-A"), fp("zip-B"));
        assertTrue(fp("zip-A").startsWith(SourceFingerprint.IMPORT_LOGIC_VERSION + ":"));
    }

    @Test
    void unchangedRecentBuildIsSkipped() throws Exception {
        recordedHoursAgo(fp("zip-A"), 5);
        assertTrue(SourceFingerprint.canSkipRebuild(record, fp("zip-A"), db, maxAge, now));
    }

    @Test
    void changedSourceIsRebuilt() throws Exception {
        recordedHoursAgo(fp("zip-A"), 5);
        assertFalse(SourceFingerprint.canSkipRebuild(record, fp("zip-B"), db, maxAge, now));
    }

    @Test
    void unchangedSourceIsStillRebuiltOnceTheBuildIsTooOld() throws Exception {
        recordedHoursAgo(fp("zip-A"), 21);
        assertFalse(SourceFingerprint.canSkipRebuild(record, fp("zip-A"), db, maxAge, now));
        recordedHoursAgo(fp("zip-A"), 19);
        assertTrue(SourceFingerprint.canSkipRebuild(record, fp("zip-A"), db, maxAge, now));
    }

    @Test
    void aFingerprintFromAnOlderImportLogicNeverMatches() throws Exception {
        String sha = fp("zip-A").substring(fp("zip-A").indexOf(':') + 1);
        recordedHoursAgo((SourceFingerprint.IMPORT_LOGIC_VERSION - 1) + ":" + sha, 1);
        assertFalse(SourceFingerprint.canSkipRebuild(record, fp("zip-A"), db, maxAge, now));
    }

    @Test
    void missingRecordMissingDatabaseOrFailedProbeMeansRebuild() throws Exception {
        assertFalse(SourceFingerprint.canSkipRebuild(record, fp("zip-A"), db, maxAge, now), "no record yet");
        recordedHoursAgo(fp("zip-A"), 1);
        Files.delete(db);
        assertFalse(SourceFingerprint.canSkipRebuild(record, fp("zip-A"), db, maxAge, now), "no live database");
        Files.writeString(db, "x");
        assertFalse(SourceFingerprint.canSkipRebuild(record, null, db, maxAge, now), "probe failed");
    }

    @Test
    void unreachableSourceYieldsNoFingerprint() {
        assertNull(GTFSFetcher.probeSourceFingerprint("invalid://url"));
    }
}
