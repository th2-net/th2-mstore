/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.mstore.cfg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

public class MessageStoreConfiguration {
    private static final long DEFAULT_DRAIN_INTERVAL = 1000L;
    private static final long DEFAULT_WAIT_TIMEOUT = 5000L;

    @JsonProperty("drain-interval")
    @JsonPropertyDescription("Interval in milliseconds to drain all aggregated batches that are not stored yet")
    private long drainInterval = DEFAULT_DRAIN_INTERVAL;

    @JsonProperty("termination-timeout")
    @JsonPropertyDescription("The timeout in milliseconds to await the inner drain scheduler has finished all tasks")
    private long terminationTimeout = DEFAULT_WAIT_TIMEOUT;

    public long getDrainInterval() {
        return drainInterval;
    }

    public void setDrainInterval(long drainInterval) {
        this.drainInterval = drainInterval;
    }

    public long getTerminationTimeout() {
        return terminationTimeout;
    }

    public void setTerminationTimeout(long terminationTimeout) {
        this.terminationTimeout = terminationTimeout;
    }
}
