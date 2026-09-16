package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Elastic step 2: keep appending while bounding unacknowledged work.
 *
 * <p>The deque stores Future handles, not rows. The SDK owns row buffering,
 * batching, and transient transport retries.
 */
public class ElasticStep2Continuous {
    static final int MAX_PENDING_EVENTS = 10_000;

    static SnowflakeStreamingIngestClient createClient() throws Exception {
        Properties properties = new Properties();
        JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(
                Paths.get(env("SNOWFLAKE_PROFILE", "profile.json"))));
        profile.fields().forEachRemaining(
                entry -> properties.put(entry.getKey(), entry.getValue().asText()));
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "continuous",
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

    static int waitAndRemoveConfirmedPrefix(Deque<CompletableFuture<Void>> pending)
            throws Exception {
        // An exceptional result is terminal from the SDK's perspective; propagate it.
        pending.peekFirst().get();
        int confirmed = 0;
        // One SDK acknowledgement may complete several consecutive append Futures.
        while (!pending.isEmpty() && pending.peekFirst().isDone()) {
            pending.removeFirst().get();
            confirmed++;
        }
        return confirmed;
    }

    static Map<String, Object> sampleRow(long eventId) {
        return Map.of(
                "EVENT_ID", eventId,
                "C1", eventId,
                "C2", "event-" + eventId);
    }

    public static void main(String[] args) throws Exception {
        long total = Long.parseLong(env("SNOWFLAKE_TEST_ROWS", "10000"));
        SnowflakeStreamingIngestClient client = createClient();
        Deque<CompletableFuture<Void>> pending = new ArrayDeque<>();
        int confirmed = 0;
        boolean completed = false;
        try {
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();
            for (long eventId = 0; eventId < total; eventId++) {
                // No callback is registered, so the returned Future identifies the append.
                pending.addLast(channel.appendRowWithWait(sampleRow(eventId), null));
                if (pending.size() >= MAX_PENDING_EVENTS) {
                    // Pause intake until at least one acknowledgement slot is released.
                    confirmed += waitAndRemoveConfirmedPrefix(pending);
                }
            }

            // End of input: wait until every accepted append is durably acknowledged.
            while (!pending.isEmpty()) {
                confirmed += waitAndRemoveConfirmedPrefix(pending);
            }
            completed = true;
            System.out.println("Durably acknowledged " + confirmed + " rows");
        } finally {
            client.close(completed, Duration.ofSeconds(60)).get(60, TimeUnit.SECONDS);
        }
    }
}
