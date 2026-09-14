package com.snowflake.example;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Append immediately; checkpoint all acknowledgements before source handoff.
 * The SDK owns batching. Caller timeouts keep the original Future alive.
 * Retain source events across restarts; Elastic replay may duplicate them.
 */
public class ElasticProducer {
    static final int CHECKPOINT_ROWS = 1000;
    static final long CHECKPOINT_NANOS = TimeUnit.SECONDS.toNanos(5);
    static final long OUTAGE_NANOS = TimeUnit.MINUTES.toNanos(5);
    static final int MAX_ATTEMPTS = 6;

    static boolean invalidation(SFException error) {
        String code = error.getErrorCodeName();
        return "InvalidChannelError".equals(code) || "InvalidClientError".equals(code)
                || "ClosedChannelError".equals(code)
                || "ClosedElasticChannelError".equals(code) || "ClosedClientError".equals(code);
    }

    static boolean retryable(SFException error) {
        int status = error.getHttpStatusCode();
        return invalidation(error) || status == 408 || status == 429
                || status == 500 || status == 502 || status == 503 || status == 504;
    }

    static long remaining(long deadline) throws TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new TimeoutException("Outage deadline exceeded; retain events after source checkpoint for replay");
        }
        return nanos;
    }

    static void backoff(int attempt, long deadline) throws Exception {
        long cap = Math.min(10000, 250L << Math.min(attempt, 6));
        long delay = TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(cap + 1));
        TimeUnit.NANOSECONDS.sleep(Math.min(delay, remaining(deadline)));
    }

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

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

    static class Event {
        final long offset;
        final Map<String, Object> row;
        Event(long offset) {
            this.offset = offset;
            this.row = Map.of("EVENT_ID", offset, "C1", offset, "C2", "event-" + offset);
        }
    }

    /** Deterministic replay fixture; its source checkpoint is not persisted. */
    static class ReplaySource {
        final long total;
        long committed;
        long nextOffset;
        ReplaySource(long total, long checkpoint) {
            if (checkpoint < 0 || checkpoint > total) throw new IllegalArgumentException("Invalid checkpoint");
            this.total = total;
            this.committed = checkpoint;
            this.nextOffset = checkpoint + 1;
        }
        Event read() { return nextOffset > total ? null : new Event(nextOffset++); }
        void acknowledge(long offset) {
            if (offset < committed || offset > total) throw new IllegalArgumentException("Invalid checkpoint");
            committed = offset;
        }
        void seek(long offset) {
            acknowledge(offset);
            nextOffset = offset + 1;
        }
    }


    interface ClientFactory { SnowflakeStreamingIngestClient create() throws Exception; }

    static class Pending {
        final Event event;
        final CompletableFuture<Void> future;
        final int generation;
        Pending(Event event, CompletableFuture<Void> future, int generation) {
            this.event = event;
            this.future = future;
            this.generation = generation;
        }
    }

    static class Producer {
        final ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestElasticChannel channel;
        int generation;
        Producer(ClientFactory factory) { this.factory = factory; }
        void open() throws Exception {
            SnowflakeStreamingIngestClient fresh = factory.create();
            try {
                channel = fresh.getElasticChannel();
            } catch (RuntimeException error) {
                fresh.close(false, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                throw error;
            }
            client = fresh;
            generation++;
        }
        void recover(int failedGeneration) throws Exception {
            // Several failures from one old client must trigger only one replacement.
            if (generation != failedGeneration) return;
            close(false);
            open();
        }
        void close(boolean flush) throws Exception {
            if (client == null) return;
            try {
                client.close(flush, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            } finally {
                client = null;
            }
        }
    }

    static Pending appendEvent(Producer producer, Event event, long deadline) throws Exception {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            remaining(deadline);
            try {
                return new Pending(event, producer.channel.appendRowWithWait(event.row, String.valueOf(event.offset)),
                        producer.generation);
            } catch (SFException error) {
                if (!retryable(error) || attempt == MAX_ATTEMPTS - 1) throw error;
                if (invalidation(error)) producer.recover(producer.generation);
                backoff(attempt, deadline);
            }
        }
        throw new IllegalStateException("Submission retry budget exhausted");
    }

    static void confirmCheckpoint(Producer producer, List<Pending> pending, ReplaySource source,
                           long deadline) throws Exception {
        for (Pending original : pending) {
            Pending item = original;
            int retries = 0;
            while (true) {
                long budget = remaining(deadline);
                try {
                    item.future.get(Math.min(TimeUnit.SECONDS.toNanos(1), budget), TimeUnit.NANOSECONDS);
                    break;
                } catch (TimeoutException waiting) {
                    // Keep the original Future; a caller timeout is not an SDK failure.
                } catch (ExecutionException failure) {
                    Throwable cause = failure.getCause();
                    if (!(cause instanceof SFException)) throw failure;
                    SFException error = (SFException) cause;
                    if (!retryable(error) || retries >= MAX_ATTEMPTS - 1) throw error;
                    if (invalidation(error)) producer.recover(item.generation);
                    System.err.println("Replaying EVENT_ID=" + item.event.offset + "; duplicates possible");
                    backoff(retries++, deadline);
                    item = appendEvent(producer, item.event, deadline);
                }
            }
        }
        if (!pending.isEmpty()) {
            // All original acknowledgements succeeded before source progress advances.
            source.acknowledge(pending.get(pending.size() - 1).event.offset);
            pending.clear();
        }
    }

    static void run(Producer producer, ReplaySource source) throws Exception {
        List<Pending> pending = new ArrayList<>();
        long checkpointAt = System.nanoTime() + CHECKPOINT_NANOS;
        long deadline = System.nanoTime() + OUTAGE_NANOS;
        Event event;
        while ((event = source.read()) != null) {
            pending.add(appendEvent(producer, event, deadline));
            if (pending.stream().anyMatch(item -> item.future.isCompletedExceptionally())
                    || pending.size() >= CHECKPOINT_ROWS || System.nanoTime() >= checkpointAt) {
                confirmCheckpoint(producer, pending, source, deadline);
                checkpointAt = System.nanoTime() + CHECKPOINT_NANOS;
                deadline = System.nanoTime() + OUTAGE_NANOS;
            }
        }
        confirmCheckpoint(producer, pending, source, deadline);
    }

    public static void main(String[] args) throws Exception {
        ReplaySource source = new ReplaySource(
                Long.parseLong(env("SNOWFLAKE_TEST_ROWS", "10000")),
                Long.parseLong(env("SNOWFLAKE_SOURCE_CHECKPOINT", "0")));
        Producer producer = new Producer(ElasticProducer::createClient);
        boolean completed = false;
        try {
            producer.open();
            run(producer, source);
            completed = true;
            System.out.println("Durable source checkpoint: " + source.committed + "; materialization is separate");
        } finally {
            if (!completed) System.err.println("Retain source events after checkpoint " + source.committed);
            producer.close(completed);
        }
    }
}
