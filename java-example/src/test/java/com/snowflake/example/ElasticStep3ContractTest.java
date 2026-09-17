package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

abstract class ElasticStep3ContractTest {
    abstract void ingest(
            ElasticStep3.ClientFactory factory, ElasticStep3.SampleEventSource source)
            throws Exception;

    abstract void ingest(
            ElasticStep3.ClientFactory factory,
            ElasticStep3.SampleEventSource source,
            int maxPending,
            long stallNanos) throws Exception;

    @Test
    void checkpointsEveryEventInOrder() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(4, 0);

        ingest(factory, source);

        assertEquals(List.of(1L, 2L, 3L, 4L), factory.appends());
        assertEquals(4, source.committed);
        assertEquals(1, factory.clients.size());
        assertEquals(List.of(true), factory.clients.get(0).flushes);
    }

    @Test
    void laterAcknowledgementDoesNotSkipAnEarlierGap() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.holdAt = 1L;
        factory.releaseHoldAt = 3L;
        ElasticStep3Fakes.RecordingSource source =
                new ElasticStep3Fakes.RecordingSource(3, 0, factory);

        ingest(factory, source);

        assertEquals(3, source.committed);
        assertEquals(List.of(1L, 2L, 3L), factory.appends());
    }

    @Test
    void backpressureRetriesTheHeldEventWithoutReplacingTheClient() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.backpressure = 1;
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(2, 0);

        ingest(factory, source);

        assertEquals(List.of(1L, 1L, 2L), factory.appends());
        assertEquals(1, factory.clients.size());
        assertEquals(2, source.committed);
    }

    @Test
    void invalidationResubmitsOnlyUnconfirmedEventsOnce() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.failAckAt = 3L;
        factory.failAckWith = ElasticStep3Fakes.error("InvalidClientError", 409);
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(4, 0);

        ingest(factory, source);

        assertEquals(List.of(1L, 2L, 3L, 3L, 4L), factory.appends());
        assertEquals(2, factory.clients.size());
        assertEquals(false, factory.clients.get(0).flushes.get(0));
        assertEquals(true, factory.clients.get(1).flushes.get(0));
        assertEquals(4, source.committed);
    }

    @Test
    void lateInvalidationFromOldClientDoesNotReplaceTheNewClient() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.failAckAt = 2L;
        factory.failAckWith = ElasticStep3Fakes.error("InvalidClientError", 409);
        factory.replayOldInvalidation = true;
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(3, 0);

        ingest(factory, source);

        assertEquals(2, factory.clients.size());
        assertEquals(3, source.committed);
        assertEquals(List.of(1L, 2L, 2L, 3L), factory.appends());
    }

    @Test
    void retryableAckFailureReplaysTheLastEventAfterEof() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.failAckAt = 2L;
        factory.failAckWith = ElasticStep3Fakes.error("TransientError", 503);
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(2, 0);

        ingest(factory, source);

        assertEquals(List.of(1L, 2L, 2L), factory.appends());
        assertEquals(1, factory.clients.size());
        assertEquals(2, source.committed);
    }

    @Test
    void retryableAckFailureReplaysTheSameEvent() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.failAckAt = 1L;
        factory.failAckWith = ElasticStep3Fakes.error("TransientError", 503);
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(2, 0);

        ingest(factory, source);

        assertEquals(List.of(1L, 1L, 2L), factory.appends());
        assertEquals(1, factory.clients.size());
        assertEquals(2, source.committed);
    }

    @Test
    void submitInvalidationReplacesTheClientAndRetriesTheHeldEvent() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.throwAt = 2L;
        factory.throwWith = ElasticStep3Fakes.error("InvalidClientError", 409);
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(3, 0);

        ingest(factory, source);

        assertEquals(List.of(1L, 2L, 2L, 3L), factory.appends());
        assertEquals(2, factory.clients.size());
        assertEquals(3, source.committed);
    }

    @Test
    void transientSubmitFailureRetriesWithoutReplacingTheClient() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.throwAt = 1L;
        factory.throwWith = ElasticStep3Fakes.error("TransientError", 500);
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(1, 0);

        ingest(factory, source);

        assertEquals(List.of(1L, 1L), factory.appends());
        assertEquals(1, factory.clients.size());
        assertEquals(1, source.committed);
    }

    @Test
    void terminalErrorKeepsTheLastCheckpoint() {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.throwAt = 2L;
        factory.throwWith = ElasticStep3Fakes.error("InvalidArgument", 400);
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(3, 0);

        SFException error = assertThrows(SFException.class, () -> ingest(factory, source));

        assertEquals("InvalidArgument", error.getErrorCodeName());
        assertEquals(1, source.committed);
        assertEquals(List.of(1L, 2L), factory.appends());
        assertEquals(List.of(false), factory.clients.get(0).flushes);
    }

    @Test
    void pendingLimitPausesIntakeUntilTimeout() {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.completeImmediately = false;
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(5, 0);

        assertThrows(
                TimeoutException.class,
                () -> ingest(factory, source, 2, TimeUnit.MILLISECONDS.toNanos(200)));

        assertEquals(List.of(1L, 2L), factory.appends());
        assertEquals(0, source.committed);
        assertEquals(List.of(false), factory.clients.get(0).flushes);
    }

    @Test
    void restartResumesAfterThePersistedCheckpoint() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(5, 2);

        ingest(factory, source);

        assertEquals(List.of(3L, 4L, 5L), factory.appends());
        assertEquals(5, source.committed);
    }

    @Test
    void checkpointWriteFailureLeavesThePreviousCheckpoint() {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(3, 0) {
            @Override
            void acknowledge(long position) {
                if (position == 2) {
                    throw new IllegalStateException("checkpoint write failed");
                }
                super.acknowledge(position);
            }
        };

        IllegalStateException error =
                assertThrows(IllegalStateException.class, () -> ingest(factory, source));

        assertEquals("checkpoint write failed", error.getMessage());
        assertEquals(1, source.committed);
    }

    @Test
    void channelOpenFailureClosesThePartialClient() {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        factory.failChannel = true;
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(1, 0);

        SFException error = assertThrows(SFException.class, () -> ingest(factory, source));

        assertEquals("InvalidClientError", error.getErrorCodeName());
        assertEquals(1, factory.clients.size());
        assertEquals(List.of(false), factory.clients.get(0).flushes);
        assertEquals(0, source.committed);
        assertTrue(factory.appends().isEmpty());
    }
}
