package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Shared source, retry, and client helpers for the two Elastic step 3 examples.
 *
 * <p>{@link ElasticStep3Futures} and {@link ElasticStep3Callbacks} differ only in
 * how they observe acknowledgements. Checkpointing, retries, and client
 * replacement follow the same rules.
 */
final class ElasticStep3 {
    static final int MAX_PENDING_EVENTS = 100_000;
    static final int MAX_ATTEMPTS = 6;
    static final long POLL_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final long MAX_NO_PROGRESS_NANOS = TimeUnit.MINUTES.toNanos(30);
    static final List<String> INVALIDATION_ERRORS = List.of(
            "InvalidChannelError",
            "InvalidClientError",
            "ClosedChannelError",
            "ClosedElasticChannelError",
            "ClosedClientError");

    private ElasticStep3() {}

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    static SnowflakeStreamingIngestClient createClient() throws Exception {
        Properties properties = new Properties();
        JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(
                Paths.get(env("SNOWFLAKE_PROFILE", "profile.json"))));
        profile.fields().forEachRemaining(
                entry -> properties.put(entry.getKey(), entry.getValue().asText()));
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "elastic-step3-" + ProcessHandle.current().pid(),
                env("SNOWFLAKE_DATABASE", "MY_DATABASE"),
                env("SNOWFLAKE_SCHEMA", "MY_SCHEMA"),
                env("SNOWFLAKE_TABLE", "MY_TABLE"))
                .setProperties(properties)
                .build();
    }

    /** One retained source event. {@code EVENT_ID} is stable across replays. */
    static final class Event {
        final long position;
        final Map<String, Object> row;

        Event(long position) {
            this.position = position;
            this.row = Map.of(
                    "EVENT_ID", position,
                    "C1", position,
                    "C2", "event-" + position);
        }
    }

    /**
     * Regenerable in-memory source. Not durable storage: replace {@link #read}
     * and {@link #acknowledge} with your log, outbox, or other retained API.
     *
     * <p>Reading does not acknowledge. A restart continues after {@code
     * committed}. {@code read()} returns {@code null} at end-of-input and never
     * blocks; a live source must still bound or interrupt its reads.
     */
    static class SampleEventSource {
        final long total;
        long committed;
        long next;

        SampleEventSource(long total, long checkpoint) {
            if (checkpoint < 0 || checkpoint > total) {
                throw new IllegalArgumentException("Require 0 <= checkpoint <= total");
            }
            this.total = total;
            this.committed = checkpoint;
            this.next = checkpoint + 1;
        }

        Event read() {
            return next > total ? null : new Event(next++);
        }

        void acknowledge(long position) {
            if (position < committed || position > total) {
                throw new IllegalArgumentException("Checkpoints may only move forward");
            }
            committed = position;
        }
    }

    interface ClientFactory {
        SnowflakeStreamingIngestClient create() throws Exception;
    }

    static final class Session {
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestElasticChannel channel;
    }

    static Session open(ClientFactory factory) throws Exception {
        SnowflakeStreamingIngestClient client = null;
        try {
            client = factory.create();
            Session session = new Session();
            session.client = client;
            session.channel = client.getElasticChannel();
            return session;
        } catch (Exception error) {
            if (client != null) {
                try {
                    closeClient(client, false);
                } catch (Exception closeError) {
                    error.addSuppressed(closeError);
                }
            }
            throw error;
        }
    }

    static Session replace(
            ClientFactory factory,
            Session current,
            SnowflakeStreamingIngestClient old) throws Exception {
        if (current.client != old) {
            return current;
        }
        current.client = null;
        current.channel = null;
        closeClient(old, false);
        return open(factory);
    }

    static void closeClient(SnowflakeStreamingIngestClient client, boolean flush)
            throws Exception {
        if (client == null) {
            return;
        }
        client.close(flush, Duration.ofSeconds(60)).get(60, TimeUnit.SECONDS);
    }

    static boolean isInvalidation(SFException error) {
        return INVALIDATION_ERRORS.contains(error.getErrorCodeName());
    }

    static boolean isBackpressure(SFException error) {
        return error.getHttpStatusCode() == 429;
    }

    static boolean isRetryable(SFException error) {
        int status = error.getHttpStatusCode();
        return isInvalidation(error)
                || status == 408
                || status == 429
                || status == 500
                || status == 502
                || status == 503
                || status == 504;
    }

    static long remaining(long deadline) throws TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new TimeoutException(
                    "No confirmed progress before the deadline; retain unconfirmed events");
        }
        return nanos;
    }

    static void backoff(int attempt, long deadline) throws Exception {
        long capMillis = Math.min(10_000, 250L << Math.min(attempt, 6));
        long delay = TimeUnit.MILLISECONDS.toNanos(
                ThreadLocalRandom.current().nextLong(capMillis + 1));
        TimeUnit.NANOSECONDS.sleep(Math.min(delay, remaining(deadline)));
    }

    static SFException sdkError(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException
                || current instanceof ExecutionException) {
            current = current.getCause();
        }
        return current instanceof SFException ? (SFException) current : null;
    }

    static SFException failure(CompletableFuture<Void> ack) {
        try {
            ack.getNow(null);
            return null;
        } catch (CompletionException error) {
            SFException sdk = sdkError(error);
            if (sdk != null) {
                return sdk;
            }
            throw new IllegalStateException("Unexpected acknowledgement failure", error);
        }
    }
}
