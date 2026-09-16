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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Elastic step 3: retain source events until acknowledgements confirm progress.
 *
 * <p>Replaying an Elastic append can create a duplicate, so a real source must
 * retain events and provide a stable event ID.
 */
public class ElasticStep3Production {
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

    // Connection and source model

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
            // Replace this with the source's durable checkpoint operation.
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

    static class Pending {
        final Event event;
        final CompletableFuture<Void> future;
        final SnowflakeStreamingIngestClient client;
        int retries;

        Pending(
                Event event,
                CompletableFuture<Void> future,
                SnowflakeStreamingIngestClient client,
                int retries) {
            this.event = event;
            this.future = future;
            this.client = client;
            this.retries = retries;
        }
    }

    // Client lifecycle

    interface ClientFactory {
        SnowflakeStreamingIngestClient create() throws Exception;
    }

    static class ElasticProducer {
        final ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestElasticChannel channel;

        ElasticProducer(ClientFactory factory) {
            this.factory = factory;
        }

        void open() throws Exception {
            SnowflakeStreamingIngestClient fresh = factory.create();
            try {
                channel = fresh.getElasticChannel();
            } catch (RuntimeException error) {
                fresh.close(false, Duration.ZERO).get();
                throw error;
            }
            client = fresh;
        }

        void swapClient(SnowflakeStreamingIngestClient failedClient) throws Exception {
            // Late failures from an old client must not close its replacement.
            if (failedClient != client) {
                return;
            }
            close(false);
            open();
        }

        void close(boolean flush) throws Exception {
            if (client == null) {
                return;
            }
            try {
                client.close(flush, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            } finally {
                client = null;
                channel = null;
            }
        }
    }

    // Retry policy

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

    // Append and acknowledgement handling

    static Pending appendEvent(
            ElasticProducer producer, Event event, long deadline, int retries)
            throws Exception {
        int attempt = retries;
        while (true) {
            remaining(deadline);
            try {
                CompletableFuture<Void> future = producer.channel.appendRowWithWait(
                        event.row, String.valueOf(event.offset));
                return new Pending(event, future, producer.client, attempt);
            } catch (SFException error) {
                if (error.getHttpStatusCode() == 429) {
                    backoff(2, deadline);
                    continue;
                }
                if (!isRetryable(error) || attempt >= MAX_ATTEMPTS - 1) {
                    throw error;
                }
                if (isInvalidation(error)) {
                    producer.swapClient(producer.client);
                }
                backoff(attempt++, deadline);
            }
        }
    }

    static Pending appendEvent(ElasticProducer producer, Event event, long deadline)
            throws Exception {
        return appendEvent(producer, event, deadline, 0);
    }

    static void collectProgress(
            ElasticProducer producer,
            List<Pending> pending,
            SampleEventSource source,
            long deadline,
            boolean wait)
            throws Exception {
        if (pending.isEmpty()) {
            return;
        }

        Pending first = pending.get(0);
        if (wait && !first.future.isDone()) {
            try {
                first.future.get(
                        Math.min(POLL_NANOS, remaining(deadline)), TimeUnit.NANOSECONDS);
            } catch (TimeoutException timeout) {
                return;
            } catch (ExecutionException completedExceptionally) {
                // Handle this terminal outcome below using the same path as an already-done Future.
            }
        }

        int confirmed = 0;
        for (Pending item : pending) {
            if (!item.future.isDone()) {
                break;
            }
            try {
                item.future.get();
            } catch (ExecutionException failure) {
                if (confirmed > 0) {
                    break;
                }
                if (!(failure.getCause() instanceof SFException)) {
                    throw failure;
                }
                SFException error = (SFException) failure.getCause();
                if (!isRetryable(error) || item.retries >= MAX_ATTEMPTS - 1) {
                    throw error;
                }
                if (isInvalidation(error)) {
                    producer.swapClient(item.client);
                }
                backoff(item.retries, deadline);
                pending.set(0, appendEvent(
                        producer, item.event, deadline, item.retries + 1));
                return;
            }
            confirmed++;
        }

        if (confirmed > 0) {
            source.acknowledge(pending.get(confirmed - 1).event.offset);
            pending.subList(0, confirmed).clear();
        }
    }

    // Ingestion flow

    static void run(ElasticProducer producer, SampleEventSource source) throws Exception {
        List<Pending> pending = new ArrayList<>();
        boolean exhausted = false;
        long deadline = 0;

        while (true) {
            if (!pending.isEmpty()) {
                long previous = source.committed;
                boolean mustWait = exhausted || pending.size() >= MAX_PENDING_EVENTS;
                collectProgress(producer, pending, source, deadline, mustWait);
                if (source.committed > previous) {
                    deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
                }
            }

            if (exhausted && pending.isEmpty()) {
                return;
            }
            if (pending.isEmpty()) {
                deadline = 0;
            } else {
                remaining(deadline);
            }
            if (exhausted || pending.size() >= MAX_PENDING_EVENTS) {
                continue;
            }

            Event event = source.read();
            if (event == null) {
                exhausted = true;
                continue;
            }
            if (deadline == 0) {
                deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
            }
            pending.add(appendEvent(producer, event, deadline));
        }
    }

    public static void main(String[] args) throws Exception {
        SampleEventSource source = new SampleEventSource(
                Long.parseLong(env("SNOWFLAKE_TEST_ROWS", "10000")),
                Long.parseLong(env("SNOWFLAKE_SOURCE_CHECKPOINT", "0")));
        ElasticProducer producer = new ElasticProducer(ElasticStep3Production::createClient);
        boolean completed = false;
        try {
            producer.open();
            run(producer, source);
            completed = true;
            System.out.println("Durable source checkpoint: " + source.committed);
        } finally {
            if (!completed) {
                System.err.println(
                        "Retain source events after checkpoint " + source.committed);
            }
            producer.close(completed);
        }
    }
}
