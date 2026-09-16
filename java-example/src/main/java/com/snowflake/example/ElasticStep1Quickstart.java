package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Elastic step 1: append one row and wait for durable acknowledgement. */
public class ElasticStep1Quickstart {
    private static final String DATABASE = env("SNOWFLAKE_DATABASE", "MY_DATABASE");
    private static final String SCHEMA = env("SNOWFLAKE_SCHEMA", "MY_SCHEMA");
    private static final String TABLE = env("SNOWFLAKE_TABLE", "MY_TABLE");

    static SnowflakeStreamingIngestClient createClient() throws Exception {
        Properties properties = new Properties();
        JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(
                Paths.get(env("SNOWFLAKE_PROFILE", "profile.json"))));
        profile.fields().forEachRemaining(
                entry -> properties.put(entry.getKey(), entry.getValue().asText()));
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "quickstart", DATABASE, SCHEMA, TABLE)
                .setProperties(properties)
                .build();
    }

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    public static void main(String[] args) throws Exception {
        SnowflakeStreamingIngestClient client = createClient();
        try {
            // Elastic Channels belong to their client and are not closed separately.
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();
            Map<String, Object> row = Map.of(
                    "DATA", Map.of("event_id", 1, "status", "active"),
                    "C1", 1,
                    "C2", "example");

            // The Future completes when Snowflake durably accepts this append.
            channel.appendRowWithWait(row, "event-1").get();
            System.out.println("Row durably acknowledged");
        } finally {
            client.close(true, Duration.ofSeconds(60)).get(60, TimeUnit.SECONDS);
        }
    }
}
