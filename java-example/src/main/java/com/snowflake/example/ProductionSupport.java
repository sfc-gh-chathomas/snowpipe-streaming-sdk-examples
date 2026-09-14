package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class ProductionSupport {
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

    static ReplaySource sourceFromEnv() {
        return new ReplaySource(Long.parseLong(env("SNOWFLAKE_TEST_ROWS", "10000")),
                Long.parseLong(env("SNOWFLAKE_SOURCE_CHECKPOINT", "0")));
    }

    interface ClientFactory { SnowflakeStreamingIngestClient create() throws Exception; }
}
