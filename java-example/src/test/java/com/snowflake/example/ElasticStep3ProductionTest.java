package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ElasticStep3ProductionTest {
    static SFException failure(String code, int status) {
        return new SFException(code, "synthetic", status, String.valueOf(status));
    }

    static CompletableFuture<Void> failed(String code, int status) {
        return CompletableFuture.failedFuture(failure(code, status));
    }

    static long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    }

    static class FakeClient {
        final List<String> calls = new ArrayList<>();
        final List<Object> outcomes = new ArrayList<>();
        int closes;

        FakeClient(Object... outcomes) {
            this.outcomes.addAll(Arrays.asList(outcomes));
        }

        final SnowflakeStreamingIngestElasticChannel channel =
                (SnowflakeStreamingIngestElasticChannel) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {SnowflakeStreamingIngestElasticChannel.class},
                        (proxy, method, args) -> {
                            if ("appendRowWithWait".equals(method.getName())) {
                                calls.add((String) args[1]);
                                Object outcome = outcomes.isEmpty()
                                        ? CompletableFuture.completedFuture(null)
                                        : outcomes.remove(0);
                                if (outcome instanceof SFException) {
                                    throw (SFException) outcome;
                                }
                                return outcome;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });

        final SnowflakeStreamingIngestClient client =
                (SnowflakeStreamingIngestClient) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {SnowflakeStreamingIngestClient.class},
                        (proxy, method, args) -> {
                            if ("getElasticChannel".equals(method.getName())) {
                                return channel;
                            }
                            if ("close".equals(method.getName())) {
                                closes++;
                                return CompletableFuture.completedFuture(null);
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    static ElasticStep3Production.ElasticProducer producer(FakeClient... clients)
            throws Exception {
        AtomicInteger index = new AtomicInteger();
        ElasticStep3Production.ElasticProducer producer =
                new ElasticStep3Production.ElasticProducer(
                        () -> clients[index.getAndIncrement()].client);
        producer.open();
        return producer;
    }

    @Test
    void streamsAndAcknowledgesAllEvents() throws Exception {
        FakeClient fake = new FakeClient();
        ElasticStep3Production.ElasticProducer producer = producer(fake);
        ElasticStep3Production.SampleEventSource source =
                new ElasticStep3Production.SampleEventSource(3, 0);

        ElasticStep3Production.run(producer, source);

        assertEquals(List.of("1", "2", "3"), fake.calls);
        assertEquals(3, source.committed);
    }

    @Test
    void lateAndOutOfOrderSuccessCannotCommitAGap() throws Exception {
        CompletableFuture<Void> first = new CompletableFuture<>();
        FakeClient fake = new FakeClient();
        ElasticStep3Production.ElasticProducer producer = producer(fake);
        ElasticStep3Production.SampleEventSource source =
                new ElasticStep3Production.SampleEventSource(2, 0);
        List<ElasticStep3Production.Pending> pending = new ArrayList<>();
        pending.add(new ElasticStep3Production.Pending(
                source.read(), first, producer.client, 0));
        pending.add(new ElasticStep3Production.Pending(
                source.read(), CompletableFuture.completedFuture(null), producer.client, 0));

        ElasticStep3Production.collectProgress(
                producer, pending, source, deadline(), false);
        assertEquals(0, source.committed);

        first.complete(null);
        ElasticStep3Production.collectProgress(
                producer, pending, source, deadline(), false);
        assertEquals(2, source.committed);
        assertEquals(0, fake.closes);
    }

    @Test
    void lateErrorsFromReplacedClientSwapOnlyOnce() throws Exception {
        FakeClient old = new FakeClient();
        FakeClient fresh = new FakeClient();
        ElasticStep3Production.ElasticProducer producer = producer(old, fresh);
        ElasticStep3Production.SampleEventSource source =
                new ElasticStep3Production.SampleEventSource(3, 0);
        List<ElasticStep3Production.Pending> pending = new ArrayList<>();
        pending.add(new ElasticStep3Production.Pending(
                source.read(), CompletableFuture.completedFuture(null), old.client, 0));
        pending.add(new ElasticStep3Production.Pending(
                source.read(), failed("InvalidChannelError", 409), old.client, 0));
        pending.add(new ElasticStep3Production.Pending(
                source.read(), failed("InvalidClientError", 409), old.client, 0));

        while (!pending.isEmpty()) {
            ElasticStep3Production.collectProgress(
                    producer, pending, source, deadline(), false);
        }

        assertEquals(List.of("2", "3"), fresh.calls);
        assertEquals(1, old.closes);
        assertEquals(3, source.committed);
    }

    @Test
    void backpressureRetriesOnlyRejectedEvent() throws Exception {
        SFException pressure = failure("ReceiverSaturated", 429);
        FakeClient fake = new FakeClient(
                CompletableFuture.completedFuture(null),
                pressure,
                CompletableFuture.completedFuture(null));
        ElasticStep3Production.ElasticProducer producer = producer(fake);
        ElasticStep3Production.SampleEventSource source =
                new ElasticStep3Production.SampleEventSource(2, 0);

        ElasticStep3Production.run(producer, source);

        assertEquals(List.of("1", "2", "2"), fake.calls);
        assertEquals(2, source.committed);
    }

    @Test
    void terminalErrorPreservesSourceCheckpoint() throws Exception {
        FakeClient fake = new FakeClient(failed("SfApiUserError", 400));
        ElasticStep3Production.ElasticProducer producer = producer(fake);
        ElasticStep3Production.SampleEventSource source =
                new ElasticStep3Production.SampleEventSource(1, 0);

        assertThrows(
                SFException.class,
                () -> ElasticStep3Production.run(producer, source));
        assertEquals(0, source.committed);
    }

    @Test
    void expiredDeadlineDoesNotCancelPendingFuture() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        assertThrows(
                TimeoutException.class,
                () -> ElasticStep3Production.remaining(System.nanoTime() - 1));
        assertFalse(future.isCancelled());
    }
}
