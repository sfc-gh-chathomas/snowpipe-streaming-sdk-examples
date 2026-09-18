package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class NamedChannelCheckpointTest {
    static SFException failure(String code, int status) {
        return new SFException(code, "synthetic", status, String.valueOf(status));
    }

    static ChannelStatus status(long offset, long errors) {
        return new ChannelStatus(
                "DB",
                "SCHEMA",
                "PIPE",
                "CHANNEL",
                "SUCCESS",
                String.valueOf(offset),
                Instant.EPOCH,
                offset,
                offset,
                errors,
                null,
                null,
                null,
                null,
                Instant.EPOCH);
    }

    static class Fake {
        long committed;
        long errors;
        int opens;
        int closes;
        int backpressure;
        boolean invalidateThird;
        final List<Long> calls = new ArrayList<>();

        Fake(long committed) {
            this.committed = committed;
        }

        final SnowflakeStreamingIngestChannel channel =
                (SnowflakeStreamingIngestChannel) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {SnowflakeStreamingIngestChannel.class},
                        (proxy, method, args) -> {
                            if ("appendRow".equals(method.getName())) {
                                long offset = Long.parseLong((String) args[1]);
                                calls.add(offset);
                                if (backpressure-- > 0) {
                                    throw failure("ReceiverSaturated", 429);
                                }
                                if (invalidateThird && offset == 3) {
                                    invalidateThird = false;
                                    throw failure("InvalidChannelError", 409);
                                }
                                committed = offset;
                                return null;
                            }
                            if ("getChannelStatus".equals(method.getName())) {
                                return status(committed, errors);
                            }
                            if ("close".equals(method.getName())) {
                                closes++;
                                return null;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });

        final SnowflakeStreamingIngestClient client =
                (SnowflakeStreamingIngestClient) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {SnowflakeStreamingIngestClient.class},
                        (proxy, method, args) -> {
                            if ("openChannel".equals(method.getName())) {
                                assertEquals(1, args.length);
                                opens++;
                                return new OpenChannelResult(
                                        channel, status(committed, errors));
                            }
                            if ("close".equals(method.getName())) {
                                return CompletableFuture.completedFuture(null);
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    static NamedChannelCheckpoint.NamedProducer producer(Fake fake) {
        return new NamedChannelCheckpoint.NamedProducer(() -> fake.client);
    }

    @Test
    void restartSeeksAfterServerOffset() throws Exception {
        Fake fake = new Fake(2);
        NamedChannelCheckpoint.SampleEventSource source =
                new NamedChannelCheckpoint.SampleEventSource(5, 0);

        NamedChannelCheckpoint.run(producer(fake), source);

        assertEquals(List.of(3L, 4L, 5L), fake.calls);
        assertEquals(5, source.committed);
        assertEquals(1, fake.opens);
    }

    @Test
    void invalidationReplaysOnlyBeyondCommittedOffset() throws Exception {
        Fake fake = new Fake(0);
        fake.invalidateThird = true;
        NamedChannelCheckpoint.SampleEventSource source =
                new NamedChannelCheckpoint.SampleEventSource(4, 0);

        NamedChannelCheckpoint.run(producer(fake), source);

        assertEquals(List.of(1L, 2L, 3L, 3L, 4L), fake.calls);
        assertEquals(2, fake.opens);
        assertEquals(4, source.committed);
    }

    @Test
    void backpressureRetriesCurrentEventWithoutReopen() throws Exception {
        Fake fake = new Fake(0);
        fake.backpressure = 1;
        NamedChannelCheckpoint.SampleEventSource source =
                new NamedChannelCheckpoint.SampleEventSource(1, 0);

        NamedChannelCheckpoint.run(producer(fake), source);

        assertEquals(2, fake.calls.size());
        assertEquals(1, fake.opens);
        assertEquals(1, source.committed);
    }

    @Test
    void rowErrorsPreventSourceHandoff() {
        Fake fake = new Fake(0);
        fake.errors = 1;
        NamedChannelCheckpoint.SampleEventSource source =
                new NamedChannelCheckpoint.SampleEventSource(2, 0);

        assertThrows(
                IllegalStateException.class,
                () -> NamedChannelCheckpoint.run(producer(fake), source));
        assertEquals(0, source.committed);
    }
}
