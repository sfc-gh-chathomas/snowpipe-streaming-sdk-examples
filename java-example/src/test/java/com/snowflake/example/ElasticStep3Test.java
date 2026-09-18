package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import org.junit.jupiter.api.Test;

class ElasticStep3Test {
    @Test
    void readDoesNotAcknowledgeAndEofIsNull() {
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(2, 0);

        assertEquals(1, source.read().position);
        assertEquals(0, source.committed);
        assertEquals(2, source.read().position);
        assertNull(source.read());
    }

    @Test
    void restartContinuesAfterTheCheckpoint() {
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(4, 2);

        assertEquals(3, source.read().position);
        source.acknowledge(3);
        assertEquals(3, source.committed);
    }

    @Test
    void checkpointCannotMoveBackward() {
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(2, 1);

        assertThrows(IllegalArgumentException.class, () -> source.acknowledge(0));
        assertEquals(1, source.committed);
    }

    @Test
    void stableEventIdMatchesSourcePosition() {
        assertEquals(7L, new ElasticStep3.Event(7).row.get("EVENT_ID"));
    }

    @Test
    void retryPolicyMatchesTheSpec() {
        assertTrue(ElasticStep3.isRetryable(new SFException("Transient", "x", 408, "Timeout")));
        assertTrue(ElasticStep3.isBackpressure(new SFException("Busy", "x", 429, "Too Many")));
        assertTrue(ElasticStep3.isInvalidation(new SFException("InvalidClientError", "x", 409, "Conflict")));
        assertFalse(ElasticStep3.isRetryable(new SFException("InvalidArgument", "x", 400, "Bad Request")));
    }
}
