package com.snowflake.example;

/**
 * README entry point for step 3. Delegates to {@link ElasticStep3Futures};
 * {@link ElasticStep3Callbacks} is the handler-based variant of the same loop.
 */
public class ElasticStep3Production {
    public static void main(String[] args) throws Exception {
        ElasticStep3Futures.main(args);
    }
}
