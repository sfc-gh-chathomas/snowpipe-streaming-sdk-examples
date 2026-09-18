package com.snowflake.example;

import com.snowflake.ingest.streaming.ErrorDetail;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import com.snowflake.ingest.streaming.SuccessDetail;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** SDK-shaped fake client used by both Elastic step 3 tests. */
final class ElasticStep3Fakes {
    private ElasticStep3Fakes() {}

    static SFException error(String code, int status) {
        return new SFException(code, "synthetic", status, String.valueOf(status));
    }

    static SuccessDetail successDetail(Object token) {
        return new SuccessDetail() {
            @Override
            public Iterable<Object> getAppendTokens() {
                return List.of(token);
            }

            @Override
            public String getRequestId() {
                return "req";
            }

            @Override
            public int getRetryCount() {
                return 0;
            }
        };
    }

    static ErrorDetail errorDetail(SFException error, Object token) {
        return new ErrorDetail() {
            @Override
            public Iterable<Object> getAppendTokens() {
                return List.of(token);
            }

            @Override
            public SFException getError() {
                return error;
            }

            @Override
            public String getRequestId() {
                return null;
            }

            @Override
            public int getRetryCount() {
                return 0;
            }
        };
    }

    static final class Factory implements ElasticStep3.ClientFactory {
        final List<Fake> clients = new ArrayList<>();
        int backpressure;
        Long failAckAt;
        SFException failAckWith;
        Long throwAt;
        SFException throwWith;
        Long holdAt;
        Long releaseHoldAt;
        boolean completeImmediately = true;
        boolean failChannel;
        boolean replayOldInvalidation;

        @Override
        public SnowflakeStreamingIngestClient create() {
            Fake fake = new Fake(this);
            if (clients.isEmpty()) {
                fake.backpressure = backpressure;
                fake.failAckAt = failAckAt;
                fake.failAckWith = failAckWith;
                fake.throwAt = throwAt;
                fake.throwWith = throwWith;
                fake.holdAt = holdAt;
                fake.releaseHoldAt = releaseHoldAt;
                fake.completeImmediately = completeImmediately;
                fake.failChannel = failChannel;
            }
            clients.add(fake);
            return fake.client;
        }

        List<Long> appends() {
            List<Long> appends = new ArrayList<>();
            for (Fake fake : clients) {
                appends.addAll(fake.appends);
            }
            return appends;
        }

        int closes() {
            int closes = 0;
            for (Fake fake : clients) {
                closes += fake.closes;
            }
            return closes;
        }
    }

    @SuppressWarnings("unchecked")
    static final class Fake {
        final Factory factory;
        final List<Long> appends = new ArrayList<>();
        final List<Boolean> flushes = new ArrayList<>();
        int closes;
        int backpressure;
        Long failAckAt;
        SFException failAckWith;
        Long throwAt;
        SFException throwWith;
        Long holdAt;
        Long releaseHoldAt;
        boolean completeImmediately = true;
        boolean failChannel;
        boolean holding;
        Object heldToken;
        CompletableFuture<Void> heldAck;
        Object lastToken;
        CompletableFuture<Void> lastAck;
        Consumer<SuccessDetail> successHandler;
        Consumer<ErrorDetail> errorHandler;

        final SnowflakeStreamingIngestElasticChannel channel =
                (SnowflakeStreamingIngestElasticChannel) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {SnowflakeStreamingIngestElasticChannel.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "appendRow":
                                    append((Map<String, Object>) args[0], args[1], false);
                                    return null;
                                case "appendRowWithWait":
                                    return append((Map<String, Object>) args[0], args[1], true);
                                case "setSuccessHandler":
                                    successHandler = (Consumer<SuccessDetail>) args[0];
                                    return null;
                                case "setErrorHandler":
                                    errorHandler = (Consumer<ErrorDetail>) args[0];
                                    return null;
                                case "isClosed":
                                    return closes > 0;
                                case "toString":
                                    return "fake-elastic-channel";
                                case "hashCode":
                                    return System.identityHashCode(proxy);
                                case "equals":
                                    return proxy == args[0];
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });

        final SnowflakeStreamingIngestClient client =
                (SnowflakeStreamingIngestClient) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {SnowflakeStreamingIngestClient.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "getElasticChannel":
                                    if (failChannel) {
                                        throw error("InvalidClientError", 409);
                                    }
                                    return channel;
                                case "close":
                                    if (method.getParameterCount() == 0) {
                                        closes++;
                                        flushes.add(true);
                                        return null;
                                    }
                                    closes++;
                                    flushes.add((Boolean) args[0]);
                                    return CompletableFuture.completedFuture(null);
                                case "isClosed":
                                    return closes > 0;
                                case "toString":
                                    return "fake-client";
                                case "hashCode":
                                    return System.identityHashCode(proxy);
                                case "equals":
                                    return proxy == args[0];
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });

        Fake(Factory factory) {
            this.factory = factory;
        }

        boolean holdingUnresolved() {
            return holding;
        }

        private CompletableFuture<Void> append(
                Map<String, Object> row, Object token, boolean wait) {
            long position = ((Number) row.get("EVENT_ID")).longValue();
            appends.add(position);
            if (backpressure > 0) {
                backpressure--;
                throw error("ReceiverSaturated", 429);
            }
            if (throwAt != null && throwAt == position) {
                throwAt = null;
                throw throwWith;
            }

            replayLateInvalidation();

            CompletableFuture<Void> ack = wait ? new CompletableFuture<>() : null;
            lastToken = token;
            lastAck = ack;

            if (failAckAt != null && failAckAt == position) {
                failAckAt = null;
                completeError(token, ack, failAckWith);
                return ack;
            }
            if (holdAt != null && holdAt == position) {
                holding = true;
                heldToken = token;
                heldAck = ack;
                return ack;
            }
            if (completeImmediately) {
                completeSuccess(token, ack);
            }
            if (releaseHoldAt != null && releaseHoldAt == position && holding) {
                holding = false;
                completeSuccess(heldToken, heldAck);
                heldToken = null;
                heldAck = null;
            }
            return ack;
        }

        private void replayLateInvalidation() {
            if (!factory.replayOldInvalidation || factory.clients.size() < 2) {
                return;
            }
            Fake first = factory.clients.get(0);
            if (first == this || first.lastToken == null) {
                return;
            }
            factory.replayOldInvalidation = false;
            first.completeError(
                    first.lastToken,
                    first.lastAck,
                    error("InvalidClientError", 409));
        }

        private void completeSuccess(Object token, CompletableFuture<Void> ack) {
            if (ack != null) {
                ack.complete(null);
            }
            if (successHandler != null && token != null) {
                successHandler.accept(successDetail(token));
            }
        }

        private void completeError(
                Object token, CompletableFuture<Void> ack, SFException error) {
            if (ack != null) {
                ack.completeExceptionally(error);
            }
            if (errorHandler != null && token != null) {
                errorHandler.accept(errorDetail(error, token));
            }
        }
    }

    static class RecordingSource extends ElasticStep3.SampleEventSource {
        final Factory factory;
        final List<Long> checkpoints = new ArrayList<>();

        RecordingSource(long total, long checkpoint, Factory factory) {
            super(total, checkpoint);
            this.factory = factory;
        }

        @Override
        void acknowledge(long position) {
            for (Fake fake : factory.clients) {
                if (fake.holdingUnresolved()) {
                    throw new AssertionError(
                            "advanced checkpoint " + position + " past an unresolved event");
                }
            }
            super.acknowledge(position);
            checkpoints.add(position);
        }
    }
}
