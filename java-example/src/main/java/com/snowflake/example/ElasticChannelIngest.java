package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Level 1: first ingest with pipelined acknowledgements. */
public class ElasticChannelIngest {
    static final String DATABASE = env("SNOWFLAKE_DATABASE", "MY_DATABASE");
    static final String SCHEMA = env("SNOWFLAKE_SCHEMA", "MY_SCHEMA");
    static final String TABLE = env("SNOWFLAKE_TABLE", "MY_TABLE");
    static final String PROFILE = env("SNOWFLAKE_PROFILE", "profile.json");
    static final String RUN_ID = env("SNOWFLAKE_RUN_ID", UUID.randomUUID().toString());
    /** Submit ten rows before waiting; the SDK combines transport payloads. */
    public static void main(String[] args) throws Exception {
        SnowflakeStreamingIngestClient client = createClient();
        boolean complete = false;
        try {
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();
            List<CompletableFuture<Void>> pending = new ArrayList<>();
            for (int eventId = 0; eventId < 10; eventId++) {
                pending.add(channel.appendRowWithWait(sampleRow(eventId), null));
            }
            for (CompletableFuture<Void> acknowledgement : pending) acknowledgement.get();
            complete = true;
            System.out.println("Durably acknowledged 10 rows; run=" + RUN_ID + ". Check materialization separately.");
        } finally {
            client.close(complete, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
        }
    }
    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    static Properties connectionProperties() {
        return connectionProperties(System::getenv);
    }

    static Properties connectionProperties(Function<String, String> env) {
        String pat = env.apply("SNOWFLAKE_PAT");
        if (pat == null || pat.isBlank()) {
            return null;
        }
        String account = env.apply("SNOWFLAKE_ACCOUNT");
        String url = env.apply("SNOWFLAKE_URL");
        if (account == null || account.isBlank() || url == null || url.isBlank()) {
            throw new IllegalArgumentException(
                    "PAT authentication requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
        }
        Properties properties = new Properties();
        properties.setProperty("authorization_type", "PAT");
        properties.setProperty("personal_access_token", pat);
        properties.setProperty("account", account);
        properties.setProperty("url", url);
        String role = env.apply("SNOWFLAKE_ROLE");
        if (role != null && !role.isBlank()) {
            properties.setProperty("role", role);
        }
        return properties;
    }

    static SnowflakeStreamingIngestClient createClient() throws Exception {
        return openClient();
    }

    static SnowflakeStreamingIngestClient openClient() throws Exception {
        Properties properties = connectionProperties();
        if (properties == null) {
            Properties profileProperties = new Properties();
            JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(Paths.get(PROFILE)));
            profile.fields().forEachRemaining(
                    entry -> profileProperties.put(entry.getKey(), entry.getValue().asText()));
            properties = profileProperties;
        }
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "ingest-" + UUID.randomUUID(), DATABASE, SCHEMA, TABLE)
                .setProperties(properties)
                .build();
    }

    static Map<String, Object> sampleRow(int eventId) {
        return Map.of(
                "EVENT_ID", eventId,
                "C1", eventId,
                "C2", RUN_ID + "-" + eventId);
    }

}
