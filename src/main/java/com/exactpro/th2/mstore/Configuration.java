/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.mstore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = Configuration.Builder.class)
public class Configuration {
    private long  drainInterval;
    private long  terminationTimeout;
    private int   maxTaskCount;
    private long  maxTaskDataSize;
    private int   maxRetryCount;
    private long  retryDelayBase;
    private double prefetchRatioToDrain;
    private boolean rebatching;
    private int  maxBatchSize;

    private Configuration() {
    }

    public static Configuration createDefault() {
        return builder().build();
    }

    @JsonProperty("drain-interval")
    public long getDrainInterval() {
        return drainInterval;
    }

    @JsonProperty("termination-timeout")
    public long getTerminationTimeout() {
        return terminationTimeout;
    }

    public Long getMaxTaskDataSize() {
        return maxTaskDataSize;
    }

    public Integer getMaxTaskCount() {
        return maxTaskCount;
    }

    public Integer getMaxRetryCount() {
        return maxRetryCount;
    }

    public Long getRetryDelayBase() {
        return retryDelayBase;
    }

    public double getPrefetchRatioToDrain() {
        return prefetchRatioToDrain;
    }

    public boolean isRebatching() {
        return rebatching;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private static final boolean DEFAULT_REBATCHING = true;
        private static final int DEFAULT_MAX_BATCH_SIZE = 128_000;
        private static final long DEFAULT_DRAIN_INTERVAL = 1000L;
        private static final long DEFAULT_WAIT_TIMEOUT = 5000L;
        private static final int DEFAULT_MAX_TASK_RETRIES = 1000000;
        private static final int DEFAULT_MAX_TASK_COUNT = 256;
        private static final long DEFAULT_RETRY_DELAY_BASEM_MS = 5000;
        private static final double DEFAULT_PREFETCH_RATIO_TO_DRAIN = 0.9;

        @JsonProperty("drain-interval")
        @JsonPropertyDescription("Interval in milliseconds to drain all aggregated batches that are not stored yet")
        private long drainInterval;

        @JsonProperty("termination-timeout")
        @JsonPropertyDescription("The timeout in milliseconds to await for the inner drain scheduler to finish all the tasks")
        private long terminationTimeout;

        @JsonProperty("maxTaskCount")
        private Integer maxTaskCount;

        @JsonProperty("maxTaskDataSize")
        private Long maxTaskDataSize;

        @JsonProperty("maxRetryCount")
        private Integer maxRetryCount;

        @JsonProperty("retryDelayBase")
        private Long retryDelayBase;

        @JsonProperty("rebatching")
        private Boolean rebatching;

        @JsonProperty("prefetchRatioToDrain")
        @JsonPropertyDescription("Ratio of prefetch when force drain should be called")
        private double prefetchRatioToDrain;

        @JsonProperty("maxBatchSize")
        private int  maxBatchSize;

        private Builder() {
            drainInterval = DEFAULT_DRAIN_INTERVAL;
            terminationTimeout = DEFAULT_WAIT_TIMEOUT;
            maxTaskDataSize = Runtime.getRuntime().totalMemory()  / 2;
            maxTaskCount = DEFAULT_MAX_TASK_COUNT;
            maxRetryCount = DEFAULT_MAX_TASK_RETRIES;
            retryDelayBase = DEFAULT_RETRY_DELAY_BASEM_MS;
            prefetchRatioToDrain = DEFAULT_PREFETCH_RATIO_TO_DRAIN;
            rebatching = DEFAULT_REBATCHING;
            maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        }


        public Builder withDrainInterval(long drainInterval) {
            this.drainInterval = drainInterval;
            return this;
        }

        public Builder withTerminationTimeout(long terminationTimeout) {
            this.terminationTimeout = terminationTimeout;
            return this;
        }

        public Builder withMaxTaskCount(Integer maxTaskCount) {
            this.maxTaskCount = maxTaskCount;
            return this;
        }

        public Builder withMaxTaskDataSize(Long maxTaskDataSize) {
            this.maxTaskDataSize = maxTaskDataSize;
            return this;
        }

        public Builder withMaxRetryCount(Integer maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        public Builder withRetryDelayBase(Long retryDelayBase) {
            this.retryDelayBase = retryDelayBase;
            return this;
        }

        public Builder withMaxBatchSize(Integer maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder withPrefetchRatioToDrain(double prefetchRatioToDrain) {
            this.prefetchRatioToDrain = Math.min(prefetchRatioToDrain, 1.0);
            return this;
        }

        public Builder withRebatching(boolean rebatching) {
            this.rebatching = rebatching;
            return this;
        }

        public Configuration build() {
            Configuration configuration = new Configuration();
            configuration.drainInterval = drainInterval;
            configuration.terminationTimeout = terminationTimeout;
            configuration.maxRetryCount = this.maxRetryCount;
            configuration.maxTaskDataSize = this.maxTaskDataSize;
            configuration.retryDelayBase = this.retryDelayBase;
            configuration.maxTaskCount = this.maxTaskCount;
            configuration.prefetchRatioToDrain = this.prefetchRatioToDrain;
            configuration.rebatching = this.rebatching;
            configuration.maxBatchSize = maxBatchSize;
            return configuration;
        }
    }
}
