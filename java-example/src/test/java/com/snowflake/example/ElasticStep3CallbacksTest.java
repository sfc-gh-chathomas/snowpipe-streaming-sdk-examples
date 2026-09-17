package com.snowflake.example;

class ElasticStep3CallbacksTest extends ElasticStep3ContractTest {
    @Override
    void ingest(ElasticStep3.ClientFactory factory, ElasticStep3.SampleEventSource source)
            throws Exception {
        ElasticStep3Callbacks.run(factory, source);
    }

    @Override
    void ingest(
            ElasticStep3.ClientFactory factory,
            ElasticStep3.SampleEventSource source,
            int maxPending,
            long stallNanos) throws Exception {
        ElasticStep3Callbacks.run(factory, source, maxPending, stallNanos);
    }
}
