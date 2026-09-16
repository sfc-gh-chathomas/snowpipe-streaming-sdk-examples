package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ElasticStep2ContinuousTest {
    @Test
    void removesOnlyTheConfirmedPrefix() throws Exception {
        CompletableFuture<Void> second = new CompletableFuture<>();
        Deque<CompletableFuture<Void>> pending = new ArrayDeque<>();
        pending.add(CompletableFuture.completedFuture(null));
        pending.add(second);
        pending.add(CompletableFuture.completedFuture(null));

        assertEquals(1, ElasticStep2Continuous.waitAndRemoveConfirmedPrefix(pending));
        assertEquals(2, pending.size());

        second.complete(null);
        assertEquals(2, ElasticStep2Continuous.waitAndRemoveConfirmedPrefix(pending));
        assertFalse(pending.iterator().hasNext());
    }

    @Test
    void sampleRowsUseZeroBasedEventIds() {
        assertEquals(0L, ElasticStep2Continuous.sampleRow(0).get("EVENT_ID"));
        assertEquals(1L, ElasticStep2Continuous.sampleRow(1).get("EVENT_ID"));
    }
}
