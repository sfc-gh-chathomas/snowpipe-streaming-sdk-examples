package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ElasticStep3ProductionTest {
    @Test
    void productionEntryPointUsesTheFuturesExample() throws Exception {
        ElasticStep3Fakes.Factory factory = new ElasticStep3Fakes.Factory();
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(1, 0);

        ElasticStep3Futures.run(factory, source);

        assertEquals(1, source.committed);
    }
}
