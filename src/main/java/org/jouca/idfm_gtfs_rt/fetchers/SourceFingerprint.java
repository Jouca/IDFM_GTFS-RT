package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Decides whether the static GTFS database really needs to be rebuilt.
 * <p>
 * The IDFM download endpoint exposes no {@code ETag}, {@code Last-Modified} or {@code Content-Length}, so change
 * detection is done on the content: the downloaded zip is hashed as it streams (a download takes a couple of
 * seconds) and compared with the hash recorded after the last successful build. The expensive part (enrichment,
 * {@code gtfs-import}, indexes) only runs when it differs.
 * <p>
 * Two things force a rebuild even when the zip is identical: the build is older than a maximum age (the enriched
 * GTFS also depends on other sources, platform codes and so on, that change independently of the zip), and
 * {@link #IMPORT_LOGIC_VERSION}, which must be bumped whenever the way the database is built changes (new index,
 * repaired file...), so that a deployment does not keep serving a database built the old way.
 */
public final class SourceFingerprint {

    /** Bump when the database build logic changes. Part of the fingerprint. */
    static final int IMPORT_LOGIC_VERSION = 2;

    private SourceFingerprint() {
    }

    /** Fingerprint of a downloaded zip: the import logic version and the SHA-256 of its bytes. */
    public static String of(InputStream zip) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (DigestInputStream in = new DigestInputStream(zip, digest)) {
                in.transferTo(java.io.OutputStream.nullOutputStream());
            }
            return IMPORT_LOGIC_VERSION + ":" + HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    /**
     * Whether the rebuild can be skipped: the fingerprint is unchanged since the last successful build, the live
     * database exists and that build is recent enough.
     *
     * @param recordFile file holding the fingerprint of the last successful build; its modification time is the
     *                   time of that build
     */
    public static boolean canSkipRebuild(Path recordFile, String current, Path liveDb, Duration maxAge, Instant now) {
        if (current == null || !Files.exists(liveDb) || !Files.exists(recordFile)) {
            return false;
        }
        try {
            String recorded = Files.readString(recordFile, StandardCharsets.UTF_8).trim();
            Instant builtAt = Files.getLastModifiedTime(recordFile).toInstant();
            return recorded.equals(current) && Duration.between(builtAt, now).compareTo(maxAge) < 0;
        } catch (IOException e) {
            return false;
        }
    }

    /** Records the fingerprint of the build that just succeeded (its modification time becomes the build time). */
    public static void record(Path recordFile, String fingerprint) throws IOException {
        Path tmp = recordFile.resolveSibling(recordFile.getFileName() + ".new");
        Files.writeString(tmp, fingerprint, StandardCharsets.UTF_8);
        Files.move(tmp, recordFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }
}
