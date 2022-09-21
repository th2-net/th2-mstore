package com.exactpro.th2.mstore;

import io.prometheus.client.Histogram;

public class MessageProcessorMetrics {
    private static final Histogram HISTOGRAM_PERSISTENCE_LATENCY = Histogram
            .build("th2_mstore_processor_persistence_latency", "Message persistence latency")
            .buckets(0.010, 0.020, 0.050, 0.100, 0.200, 0.300, 0.400, 0.500, 1.000, 1.500, 2.000, 2.500, 3.000, 4.000, 5.000, 10.000)
            .register();

    public Histogram.Timer startMeasuringPersistenceLatency() {

        return HISTOGRAM_PERSISTENCE_LATENCY.startTimer();
    }
}
