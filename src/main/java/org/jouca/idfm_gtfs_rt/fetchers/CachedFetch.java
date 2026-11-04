package org.jouca.idfm_gtfs_rt.fetchers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Keeps the last successfully fetched value so a frequently polled upstream is not hit on every cycle, and so a
 * temporary upstream failure does not make the data vanish.
 * <ul>
 *   <li>Within {@code ttl} of the last successful fetch, the cached value is returned without any call.</li>
 *   <li>After that the loader is called again; if it fails, the previous value is still served as long as it is
 *       younger than {@code maxStale}, and the failure is rethrown only beyond that.</li>
 * </ul>
 *
 * @param <T> type of the cached value
 */
public final class CachedFetch<T> {

    /** Fetches a fresh value from the upstream. */
    @FunctionalInterface
    public interface Loader<T> {
        T load() throws Exception;
    }

    private final Loader<T> loader;
    private final Duration ttl;
    private final Duration maxStale;
    private final Clock clock;

    private T value;
    private Instant fetchedAt;

    public CachedFetch(Loader<T> loader, Duration ttl, Duration maxStale, Clock clock) {
        this.loader = loader;
        this.ttl = ttl;
        this.maxStale = maxStale;
        this.clock = clock;
    }

    public synchronized T get() throws Exception {
        Instant now = clock.instant();
        if (value != null && Duration.between(fetchedAt, now).compareTo(ttl) < 0) {
            return value;
        }
        try {
            value = loader.load();
            fetchedAt = now;
            return value;
        } catch (Exception e) {
            if (value != null && Duration.between(fetchedAt, now).compareTo(maxStale) < 0) {
                return value;
            }
            throw e;
        }
    }
}
