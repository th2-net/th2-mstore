/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.mstore;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public class ErrorCollector implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorCollector.class);
    private final ScheduledFuture<?> drainFuture;
    private final MessageRouter<EventBatch> eventRouter;
    private final EventID rootEvent;
    private final Lock lock = new ReentrantLock();
    private Map<String, ErrorMetadata> errors = new HashMap<>();

    public ErrorCollector(@NotNull ScheduledExecutorService executor,
                          @NotNull MessageRouter<EventBatch> eventRouter,
                          @NotNull EventID rootEvent,
                          long period,
                          @NotNull TimeUnit unit) {
        this.eventRouter = requireNonNull(eventRouter, "Event router can't be null");
        this.rootEvent = requireNonNull(rootEvent, "Root event can't be null");
        requireNonNull(unit, "Unit can't be null");
        this.drainFuture = requireNonNull(executor, "Executor can't be null")
                .scheduleAtFixedRate(this::drain, period, period, unit);
    }

    public ErrorCollector(@NotNull ScheduledExecutorService executor,
                          @NotNull MessageRouter<EventBatch> eventRouter,
                          @NotNull EventID rootEvent) {
        this(executor, eventRouter, rootEvent, 1, TimeUnit.MINUTES);
    }

    /**
     * Log error and call the {@link #collect(String)}} method
     * @param error is used as key identifier. Avoid put a lot of unique values
     */
    public void collect(Logger logger, String error, Throwable cause) {
        logger.error(error, cause);
        collect(error);
    }

    /**
     * @param error is used as key identifier. Avoid put a lot of unique values
     */
    public void collect(String error) {
        lock.lock();
        try {
            errors.compute(error, (key, metadata) -> {
                if (metadata == null) {
                    return new ErrorMetadata();
                }
                metadata.inc();
                return metadata;
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        drainFuture.cancel(true);
        drain();
    }

    private void drain() {
        try {
            Map<String, ErrorMetadata> map = clear();
            if (map.isEmpty()) { return; }

            eventRouter.sendAll(Event.start()
                            .name("mstore internal problem(s): " + calculateTotalQty(map.values()))
                            .type("InternalError")
                            .status(Status.FAILED)
                            .bodyData(new BodyData(map))
                            .toBatchProto(rootEvent));

        } catch (IOException | RuntimeException e) {
            LOGGER.error("Drain events task failure", e);
        }
    }

    private Map<String, ErrorMetadata> clear() {
        lock.lock();
        try {
            Map<String, ErrorMetadata> result = errors;
            errors = new HashMap<>();
            return result;
        } finally {
            lock.unlock();
        }
    }

    private static int calculateTotalQty(Collection<ErrorMetadata> errors) {
        return errors.stream()
                .map(ErrorMetadata::getQuantity)
                .reduce(0, Integer::sum);
    }

    private static class BodyData implements IBodyData {
        private final Map<String, ErrorMetadata> errors;
        @JsonCreator
        private BodyData(Map<String, ErrorMetadata> errors) {
            this.errors = errors;
        }
        public Map<String, ErrorMetadata> getErrors() {
            return errors;
        }
    }

    private static class ErrorMetadata {
        @JsonSerialize(using = InstantSerializer.class)
        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS", timezone = "UTC")
        private final Instant firstDate = Instant.now();
        @JsonSerialize(using = InstantSerializer.class)
        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS", timezone = "UTC")
        private Instant lastDate;
        private int quantity = 1;

        public void inc() {
            quantity += 1;
            lastDate = Instant.now();
        }

        public Instant getFirstDate() {
            return firstDate;
        }

        public Instant getLastDate() {
            return lastDate;
        }

        public void setLastDate(Instant lastDate) {
            this.lastDate = lastDate;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
    }
}
