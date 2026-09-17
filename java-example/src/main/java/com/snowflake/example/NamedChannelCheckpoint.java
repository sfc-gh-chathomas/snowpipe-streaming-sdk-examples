package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Coordinate retained source offsets with one stable named channel.
 *
 * <p>The offset token is checkpoint metadata, not a deduplication key. Give
 * each channel name one owner and retain source events until committed progress
 * is confirmed.
 */
public class NamedChannelCheckpoint {
    static final int MAX_PENDING_EVENTS = 100_000;
    static final long MAX_NO_PROGRESS_NANOS = TimeUnit.MINUTES.toNanos(30);
    static final long POLL_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final int MAX_ATTEMPTS = 6;
    static final List<String> INVALIDATION_ERRORS = List.of(
            "InvalidChannelError",
            "InvalidClientError",
            "ClosedChannelError",
            "ClosedElasticChannelError",
            "ClosedClientError");
    static final String CHANNEL_NAME = env("SNOWFLAKE_CHANNEL", "production-source-0");
    static final int CHECKPOINT_ROWS = 1_000;
    static final long CHECKPOINT_NANOS = TimeUnit.SECONDS.toNanos(5);

    static SnowflakeStreamingIngestClient createClient() throws Exception {
        Properties properties = new Properties();
        JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(
                Paths.get(env("SNOWFLAKE_PROFILE", "profile.json"))));
        profile.fields().forEachRemaining(
                entry -> properties.put(entry.getKey(), entry.getValue().asText()));
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "production-" + ProcessHandle.current().pid(),
                env("SNOWFLAKE_DATABASE", "MY_DATABASE"),
                env("SNOWFLAKE_SCHEMA", "MY_SCHEMA"),
                env("SNOWFLAKE_TABLE", "MY_TABLE"))
                .setProperties(properties)
                .build();
    }

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    static class Event {
        final long offset;
        final Map<String, Object> row;

        Event(long offset) {
            this.offset = offset;
            this.row = Map.of(
                    "EVENT_ID", offset,
                    "C1", offset,
                    "C2", "event-" + offset);
        }
    }

    static class SampleEventSource {
        final long total;
        long committed;
        long nextOffset;

        SampleEventSource(long total, long checkpoint) {
            if (checkpoint < 0 || checkpoint > total) {
                throw new IllegalArgumentException("Require 0 <= checkpoint <= total");
            }
            this.total = total;
            this.committed = checkpoint;
            this.nextOffset = checkpoint + 1;
        }

        Event read() {
            return nextOffset > total ? null : new Event(nextOffset++);
        }

        void acknowledge(long offset) {
            if (offset < committed || offset > total) {
                throw new IllegalArgumentException("Invalid source checkpoint");
            }
            committed = offset;
        }

        void seek(long offset) {
            acknowledge(offset);
            nextOffset = offset + 1;
        }
    }

    interface ClientFactory {
        SnowflakeStreamingIngestClient create() throws Exception;
    }

    static boolean isInvalidation(SFException error) {
        return INVALIDATION_ERRORS.contains(error.getErrorCodeName());
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

    static long remaining(long deadline) throws java.util.concurrent.TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new java.util.concurrent.TimeoutException(
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

    static long parseOffset(String token) {
        return token == null ? 0 : Long.parseLong(token);
    }

    static class NamedProducer {
        final ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestChannel channel;

        NamedProducer(ClientFactory factory) {
            this.factory = factory;
        }

        long open() throws Exception {
            if (client == null) {
                client = factory.create();
            }
            OpenChannelResult opened = client.openChannel(CHANNEL_NAME);
            channel = opened.getChannel();
            if (opened.getChannelStatus().getRowsErrorCount() > 0) {
                throw new IllegalStateException(
                        "Row errors require reconciliation before source handoff");
            }
            return parseOffset(
                    opened.getChannelStatus().getLatestCommittedOffsetToken());
        }

        long recover(SFException error) throws Exception {
            // Reopen without replacing the server-side committed offset.
            if ("InvalidClientError".equals(error.getErrorCodeName())) {
                close(false);
            } else if (channel != null) {
                try {
                    channel.close(false, Duration.ZERO);
                } catch (SFException alreadyClosed) {
                    // The local handle is already unusable; the named channel still exists.
                }
            }

            try {
                return open();
            } catch (SFException reopened) {
                String code = reopened.getErrorCodeName();
                if (!"InvalidClientError".equals(code)
                        && !"ClosedClientError".equals(code)) {
                    throw reopened;
                }
                close(false);
                return open();
            }
        }

        void close(boolean flush) throws Exception {
            if (client == null) {
                return;
            }
            try {
                client.close(flush, Duration.ofSeconds(30)).get(
                        30, TimeUnit.SECONDS);
            } finally {
                client = null;
                channel = null;
            }
        }
    }

    static void collectProgress(
            NamedProducer producer, long submitted, SampleEventSource source) {
        ChannelStatus status = producer.channel.getChannelStatus();
        if (status.getRowsErrorCount() > 0) {
            throw new IllegalStateException(
                    "Row errors require reconciliation before source handoff");
        }
        if (!"SUCCESS".equals(status.getStatusCode())) {
            throw new SFException(
                    "InvalidChannelError", status.getStatusCode(), 409, "Conflict");
        }

        long committed = Math.min(
                submitted, parseOffset(status.getLatestCommittedOffsetToken()));
        if (committed > source.committed) {
            source.acknowledge(committed);
        }
    }

    static void run(NamedProducer producer, SampleEventSource source) throws Exception {
        // Snowflake's committed token determines where this retained source resumes.
        source.seek(producer.open());
        long submitted = source.committed;
        Event event = null;
        boolean exhausted = false;
        int failures = 0;
        int rowsSincePoll = 0;
        long nextPoll = System.nanoTime() + CHECKPOINT_NANOS;
        long deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;

        while (true) {
            try {
                boolean outstanding = submitted > source.committed;
                boolean shouldPoll = outstanding
                        && (exhausted
                        || event != null
                        || rowsSincePoll >= CHECKPOINT_ROWS
                        || System.nanoTime() >= nextPoll
                        || submitted - source.committed >= MAX_PENDING_EVENTS);
                if (shouldPoll) {
                    long previous = source.committed;
                    collectProgress(producer, submitted, source);
                    if (source.committed > previous) {
                        deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
                        failures = 0;
                    }
                    rowsSincePoll = 0;
                    nextPoll = System.nanoTime() + CHECKPOINT_NANOS;
                }

                if (submitted == source.committed && event == null) {
                    deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
                    if (exhausted) {
                        return;
                    }
                }

                remaining(deadline);
                if (exhausted
                        || submitted - source.committed >= MAX_PENDING_EVENTS) {
                    TimeUnit.NANOSECONDS.sleep(
                            Math.min(POLL_NANOS, remaining(deadline)));
                    continue;
                }

                if (event == null) {
                    event = source.read();
                }
                if (event == null) {
                    exhausted = true;
                    continue;
                }

                producer.channel.appendRow(
                        event.row, String.valueOf(event.offset));
                submitted = event.offset;
                rowsSincePoll++;
                event = null;
            } catch (SFException error) {
                if (!isRetryable(error)) {
                    throw error;
                }
                if (error.getHttpStatusCode() != 429
                        && ++failures >= MAX_ATTEMPTS) {
                    throw error;
                }
                if (isInvalidation(error)) {
                    long previous = source.committed;
                    source.seek(producer.recover(error));
                    if (source.committed > previous) {
                        deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
                    }
                    submitted = source.committed;
                    event = null;
                    exhausted = false;
                }
                backoff(2, deadline);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SampleEventSource source = new SampleEventSource(
                Long.parseLong(env("SNOWFLAKE_TEST_ROWS", "10000")),
                0);
        NamedProducer producer = new NamedProducer(NamedChannelCheckpoint::createClient);
        boolean completed = false;
        try {
            run(producer, source);
            completed = true;
            System.out.println(
                    "Committed source checkpoint: " + source.committed);
        } finally {
            if (!completed) {
                System.err.println(
                        "Retain source events after checkpoint " + source.committed);
            }
            producer.close(completed);
        }
    }
}
