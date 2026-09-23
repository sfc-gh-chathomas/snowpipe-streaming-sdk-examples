package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Send ten rows through an Elastic channel and await durable acknowledgements. */
public class ElasticChannelQuickstart {
    private static final String DATABASE = "MY_DATABASE";
    private static final String SCHEMA = "MY_SCHEMA";
    private static final String TABLE = "MY_TABLE";

    /** Load profile.json, pipeline sample rows, confirm, and close the client. */
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        JsonNode profile = new ObjectMapper().readTree(Paths.get("profile.json").toFile());
        profile.fields().forEachRemaining(entry ->
                properties.put(entry.getKey(), entry.getValue().asText()));
        SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.tableBuilder(
                "quickstart-" + UUID.randomUUID(), DATABASE, SCHEMA, TABLE)
                .setProperties(properties).build();
        boolean complete = false;
        try {
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();
            List<CompletableFuture<Void>> pending = new ArrayList<>();
            for (int eventId = 1; eventId <= 10; eventId++) {
                Map<String, Object> row = Map.of("C1", eventId, "C2", String.valueOf(eventId));
                pending.add(channel.appendRowWithWait(row, null));
            }
            for (CompletableFuture<Void> acknowledgement : pending) acknowledgement.get();
            complete = true;
            System.out.println("Durably acknowledged 10 rows. Check table contents separately.");
        } finally {
            client.close(complete, Duration.ofSeconds(30)).get();
        }
    }
}
