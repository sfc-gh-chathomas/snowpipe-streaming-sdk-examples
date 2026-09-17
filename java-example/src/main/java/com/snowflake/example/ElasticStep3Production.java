package com.snowflake.example;

/**
 * Default Elastic step 3 entry point. Uses the Futures acknowledgement API.
 * See {@link ElasticStep3Callbacks} for the handler-based variant.
 */
public class ElasticStep3Production {
    public static void main(String[] args) throws Exception {
        ElasticStep3Futures.main(args);
    }
}
