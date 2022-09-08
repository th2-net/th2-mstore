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

    private Configuration() {
    }

    public Configuration createDefault() {
        return builder().build();
    }

    public long getDrainInterval() {
        return drainInterval;
    }

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

    public static Builder builder() {
        return new Builder();
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private static final long DEFAULT_DRAIN_INTERVAL = 1000L;
        private static final long DEFAULT_WAIT_TIMEOUT = 5000L;
        private static final int DEFAULT_MAX_TASK_RETRIES = 3;
        private static final int DEFAULT_MAX_TASK_COUNT = 1024;
        private static final long DEFAULT_RETRY_DELAY_BASEM_MS = 5000;

        @JsonProperty("drain-interval")
        @JsonPropertyDescription("Interval in milliseconds to drain all aggregated batches that are not stored yet")
        private long drainInterval;

        @JsonProperty("termination-timeout")
        @JsonPropertyDescription("The timeout in milliseconds to await for the inner drain scheduler to finish all the tasks")
        private long terminationTimeout;

        private Integer maxTaskCount;
        private Long maxTaskDataSize;
        private Integer maxRetryCount;
        private Long retryDelayBase;

        private Builder() {
            drainInterval = DEFAULT_DRAIN_INTERVAL;
            terminationTimeout = DEFAULT_WAIT_TIMEOUT;
            maxTaskDataSize = Runtime.getRuntime().totalMemory()  / 2;
            maxTaskCount = DEFAULT_MAX_TASK_COUNT;
            maxRetryCount = DEFAULT_MAX_TASK_RETRIES;
            retryDelayBase = DEFAULT_RETRY_DELAY_BASEM_MS;
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

        public Configuration build() {
            Configuration configuration = new Configuration();
            configuration.drainInterval = drainInterval;
            configuration.terminationTimeout = terminationTimeout;
            configuration.maxRetryCount = this.maxRetryCount;
            configuration.maxTaskDataSize = this.maxTaskDataSize;
            configuration.retryDelayBase = this.retryDelayBase;
            configuration.maxTaskCount = this.maxTaskCount;
            return configuration;
        }
    }
}
