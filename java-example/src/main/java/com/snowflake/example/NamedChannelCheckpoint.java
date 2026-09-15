package com.snowflake.example;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.time.Duration;

/**
 * Single-writer named-channel producer. Stream rows immediately and retain source
 * events until their committed offset is confirmed. Outage pauses intake; only
 * SDK invalidation reopens. Do not share ownership of the same channel.
 */
public class NamedChannelCheckpoint {
    static final int CHECKPOINT_ROWS = 1000;
    static final long CHECKPOINT_NANOS = TimeUnit.SECONDS.toNanos(5);
    static final long OUTAGE_NANOS = TimeUnit.MINUTES.toNanos(5);
    static final int MAX_ATTEMPTS = 6;

    // Start here: create a source, connect, and stream retained events.
    public static void main(String[] args) throws Exception {
        SampleEventSource source = new SampleEventSource(
                Long.parseLong(env("SNOWFLAKE_TEST_ROWS", "10000")),
                Long.parseLong(env("SNOWFLAKE_SOURCE_CHECKPOINT", "0")));
        Producer producer = new Producer(NamedChannelCheckpoint::createClient);
        boolean completed = false;
        try {
            run(producer, source);
            completed = true;
            System.out.println("Committed source checkpoint: " + source.committed);
        } finally {
            if (!completed) System.err.println("Retain source events after checkpoint " + source.committed);
            producer.close(completed);
        }
    }

    /** Stream retained events and pause intake at delivery checkpoints. */
    static void run(Producer producer, SampleEventSource source) throws Exception {
        // The server checkpoint is authoritative when restarting this source.
        source.seek(producer.open());
        long lastSubmittedOffset = source.committed;
        int uncommittedCount = 0;
        int retryAttempts = 0;
        Event event = null;
        long deadline = System.nanoTime() + OUTAGE_NANOS;
        long checkpointAt = System.nanoTime() + CHECKPOINT_NANOS;
        while (true) {
            try {
                if (event == null) event = source.read();
                if (event == null) {
                    if (uncommittedCount > 0) confirmCheckpoint(producer, lastSubmittedOffset, source, deadline);
                    return;
                }
                remaining(deadline);
                // Write immediately; the SDK handles transport batching.
                producer.channel.appendRow(event.row, String.valueOf(event.offset));
                lastSubmittedOffset = event.offset;
                event = null;
                uncommittedCount++;
                if (uncommittedCount >= CHECKPOINT_ROWS || System.nanoTime() >= checkpointAt) {
                    confirmCheckpoint(producer, lastSubmittedOffset, source, deadline);
                    uncommittedCount = 0;
                    retryAttempts = 0;
                    deadline = System.nanoTime() + OUTAGE_NANOS;
                    checkpointAt = System.nanoTime() + CHECKPOINT_NANOS;
                }
            } catch (SFException error) {
                if (!retryable(error) || ++retryAttempts >= MAX_ATTEMPTS) throw error;
                if (invalidation(error)) {
                    source.seek(producer.recover(error));
                    lastSubmittedOffset = source.committed;
                    uncommittedCount = 0;
                    event = null;
                }
                backoff(retryAttempts - 1, deadline);
            }
        }
    }



    // Supporting delivery and connection details.
    static boolean invalidation(SFException error) {
        String code = error.getErrorCodeName();
        return "InvalidChannelError".equals(code) || "InvalidClientError".equals(code)
                || "ClosedChannelError".equals(code)
                || "ClosedElasticChannelError".equals(code) || "ClosedClientError".equals(code);
    }

    /** Identify SDK failures eligible for bounded application retry. */
    static boolean retryable(SFException error) {
        int status = error.getHttpStatusCode();
        return invalidation(error) || status == 408 || status == 429
                || status == 500 || status == 502 || status == 503 || status == 504;
    }

    /** Return the remaining checkpoint budget, or stop without advancing source progress. */
    static long remaining(long deadline) throws TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new TimeoutException("Outage deadline exceeded; retain events after source checkpoint for replay");
        }
        return nanos;
    }

    /** Wait with capped jitter without exceeding the remaining checkpoint budget. */
    static void backoff(int attempt, long deadline) throws Exception {
        long cap = Math.min(10000, 250L << Math.min(attempt, 6));
        long delay = TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(cap + 1));
        TimeUnit.NANOSECONDS.sleep(Math.min(delay, remaining(deadline)));
    }

    /** Read an optional setting, using the fallback for missing or blank values. */
    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    /** Create a table client using the authentication profile or explicitly configured PAT. */
    static SnowflakeStreamingIngestClient createClient() throws Exception {
        Properties properties = new Properties();
        String pat = System.getenv("SNOWFLAKE_PAT");
        if (pat != null && !pat.isBlank()) {
            String account = System.getenv("SNOWFLAKE_ACCOUNT");
            String url = System.getenv("SNOWFLAKE_URL");
            if (account == null || url == null) {
                throw new IllegalArgumentException("PAT mode requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
            }
            properties.put("authorization_type", "PAT");
            properties.put("personal_access_token", pat);
            properties.put("account", account);
            properties.put("url", url);
            if (System.getenv("SNOWFLAKE_ROLE") != null) {
                properties.put("role", System.getenv("SNOWFLAKE_ROLE"));
            }
        } else {
            JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(
                    Paths.get(env("SNOWFLAKE_PROFILE", "profile.json"))));
            profile.fields().forEachRemaining(entry -> properties.put(entry.getKey(), entry.getValue().asText()));
        }
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "production-" + ProcessHandle.current().pid(), env("SNOWFLAKE_DATABASE", "MY_DATABASE"),
                env("SNOWFLAKE_SCHEMA", "MY_SCHEMA"), env("SNOWFLAKE_TABLE", "MY_TABLE"))
                .setProperties(properties).build();
    }

    /** Pair a stable sample source offset with the row sent to Snowflake. */
    static class Event {
        final long offset;
        final Map<String, Object> row;
        Event(long offset) {
            this.offset = offset;
            // Replace this mapping with your table columns and stable event ID.
            this.row = Map.of("EVENT_ID", offset, "C1", offset, "C2", "event-" + offset);
        }
    }

    /** Synthetic input only. Replace read/acknowledge/seek with retained source operations. */
    static class SampleEventSource {
        final long total;
        long committed;
        long nextOffset;
        SampleEventSource(long total, long checkpoint) {
            if (checkpoint < 0 || checkpoint > total) throw new IllegalArgumentException("Invalid checkpoint");
            this.total = total;
            this.committed = checkpoint;
            this.nextOffset = checkpoint + 1;
        }
        /** Return the next sample event without acknowledging source progress. */
        Event read() { return nextOffset > total ? null : new Event(nextOffset++); }
        /** Record confirmed progress; replace with your source's durable commit operation. */
        void acknowledge(long offset) {
            // Persist source progress before retiring real source events.
            if (offset < committed || offset > total) throw new IllegalArgumentException("Invalid checkpoint");
            committed = offset;
        }
        /** Resume sample reads after confirmed progress; replace with your source seek operation. */
        void seek(long offset) {
            acknowledge(offset);
            nextOffset = offset + 1;
        }
    }


    interface ClientFactory { SnowflakeStreamingIngestClient create() throws Exception; }

    static final String CHANNEL_NAME = env("SNOWFLAKE_CHANNEL", "production-source-0");

    /** Decode this sample's numeric source offset; an absent token means no progress. */
    static long parseOffset(String token) { return token == null ? 0 : Long.parseLong(token); }

    /** Own the SDK client and channel state needed for recovery. */
    static class Producer {
        final ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestChannel channel;
        Producer(ClientFactory factory) { this.factory = factory; }
        /** Open the owned named channel and return its authoritative committed source offset. */
        long open() throws Exception {
            if (client == null) client = factory.create();
            OpenChannelResult opened = client.openChannel(CHANNEL_NAME);
            channel = opened.getChannel();
            if (opened.getChannelStatus().getRowsErrorCount() > 0) {
                throw new IllegalStateException("Reconcile row errors before source handoff");
            }
            return parseOffset(opened.getChannelStatus().getLatestCommittedOffsetToken());
        }
        /** Reopen without resetting the server offset, recreating an invalid client if needed. */
        long recover(SFException error) throws Exception {
            if ("InvalidClientError".equals(error.getErrorCodeName())) {
                close(false);
            } else if (channel != null) {
                try {
                    channel.close(false, Duration.ofSeconds(30));
                } catch (SFException alreadyInvalid) {
                    // Reopen the named channel without dropping its committed offset.
                }
            }
            try {
                return open();
            } catch (SFException reopened) {
                if (!"InvalidClientError".equals(reopened.getErrorCodeName())
                        && !"ClosedClientError".equals(reopened.getErrorCodeName())) throw reopened;
                close(false);
                return open();
            }
        }
        /** Close the current client; flush only when requested by the caller. */
        void close(boolean flush) throws Exception {
            if (client == null) return;
            try {
                client.close(flush, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            } finally {
                client = null;
            }
        }
    }

    /** Confirm committed progress and row health before acknowledging the source. */
    static void confirmCheckpoint(Producer producer, long target, SampleEventSource source,
                           long deadline) throws Exception {
        while (true) {
            remaining(deadline);
            try {
                ChannelStatus status = producer.channel.getChannelStatus();
                if (status.getRowsErrorCount() > 0) throw new IllegalStateException("Reconcile row errors before handoff");
                if (!"SUCCESS".equals(status.getStatusCode())) {
                    throw new SFException("InvalidChannelError", status.getStatusCode(), 409, "Conflict");
                }
                if (parseOffset(status.getLatestCommittedOffsetToken()) >= target) {
                    source.acknowledge(target);
                    return;
                }
            } catch (SFException error) {
                if (invalidation(error) || !retryable(error)) throw error;
            }
            backoff(2, deadline);
        }
    }


}
