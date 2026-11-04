package org.jouca.idfm_gtfs_rt.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class CachedFetchTest {

    /** A clock the test moves by hand. */
    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-04T10:00:00Z");
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
        void advance(Duration d) { now = now.plus(d); }
    }

    private final MutableClock clock = new MutableClock();
    private final AtomicInteger calls = new AtomicInteger();
    private boolean upstreamUp = true;

    private CachedFetch<String> cache() {
        return new CachedFetch<>(() -> {
            calls.incrementAndGet();
            if (!upstreamUp) throw new IOException("502");
            return "v" + calls.get();
        }, Duration.ofMinutes(3), Duration.ofHours(1), clock);
    }

    @Test
    void valueIsReusedWithinTheTtlWithoutCallingTheUpstream() throws Exception {
        CachedFetch<String> c = cache();
        assertEquals("v1", c.get());
        clock.advance(Duration.ofMinutes(2));
        assertEquals("v1", c.get());
        assertEquals(1, calls.get());
    }

    @Test
    void valueIsRefreshedOnceTheTtlIsOver() throws Exception {
        CachedFetch<String> c = cache();
        c.get();
        clock.advance(Duration.ofMinutes(3));
        assertEquals("v2", c.get());
        assertEquals(2, calls.get());
    }

    @Test
    void stalePreviousValueIsServedWhileTheUpstreamIsDownButNotForever() throws Exception {
        CachedFetch<String> c = cache();
        assertEquals("v1", c.get());
        upstreamUp = false;
        clock.advance(Duration.ofMinutes(10));
        assertEquals("v1", c.get(), "upstream down: previous value still served");
        clock.advance(Duration.ofMinutes(55));
        assertThrows(IOException.class, c::get, "older than maxStale: the failure surfaces");
    }

    @Test
    void failureWithNothingCachedIsPropagated() {
        upstreamUp = false;
        assertThrows(IOException.class, () -> cache().get());
    }

    @Test
    void recoveryAfterAnOutageFetchesAFreshValue() throws Exception {
        CachedFetch<String> c = cache();
        c.get();
        upstreamUp = false;
        clock.advance(Duration.ofMinutes(5));
        c.get();
        upstreamUp = true;
        String fresh = c.get();
        assertEquals("v" + calls.get(), fresh);
    }
}
