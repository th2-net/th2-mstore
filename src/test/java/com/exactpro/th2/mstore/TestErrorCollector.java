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

import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.google.protobuf.util.Timestamps;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
class TestErrorCollector {

    private static final long PERIOD = RandomUtils.nextLong(0, Long.MAX_VALUE);
    private static final TimeUnit TIME_UNIT = TimeUnit.values()[RandomUtils.nextInt(0, TimeUnit.values().length)] ;

    @Mock
    private Logger logger;
    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private ScheduledFuture<?> future;
    @Mock
    private MessageRouter<EventBatch> eventRouter;
    private final EventID rootEvent = EventID.newBuilder()
            .setBookName("test-book")
            .setScope("test-scope")
            .setId("test-id")
            .setStartTimestamp(Timestamps.now())
            .build();
    private ErrorCollector errorCollector;
    @Captor
    private ArgumentCaptor<Runnable> taskCaptor;

    @BeforeEach
    void beforeEach() {
        doReturn(future).when(executor).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        errorCollector = new ErrorCollector(executor, eventRouter, rootEvent, PERIOD, TIME_UNIT);
        verify(executor).scheduleAtFixedRate(taskCaptor.capture(), eq(PERIOD), eq(PERIOD), eq(TIME_UNIT));
        verifyNoMoreInteractions(executor);
    }

    @AfterEach
    void afterEach() {
        verifyNoMoreInteractions(logger);
        verifyNoMoreInteractions(executor);
        verifyNoMoreInteractions(future);
        verifyNoMoreInteractions(eventRouter);
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    @Test
    void testCollect() throws IOException {
        errorCollector.collect("A");
        for (int i = 0; i < 2; i++) {
            errorCollector.collect("B");
        }
        verifyNoMoreInteractions(eventRouter);

        taskCaptor.getValue().run();

        ArgumentCaptor<EventBatch> eventBatchCaptor = ArgumentCaptor.forClass(EventBatch.class);
        verify(eventRouter).sendAll(eventBatchCaptor.capture());

        assertEquals(1, eventBatchCaptor.getValue().getEventsCount());
        Event event = eventBatchCaptor.getValue().getEvents(0);

        assertEquals("mstore internal problem(s): 3", event.getName());
        assertEquals("InternalError", event.getType());
        assertEquals(EventStatus.FAILED, event.getStatus());

        String body = event.getBody().toStringUtf8();
        assertTrue(body.matches(".*\"A\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+\",\"quantity\":1}.*"), () -> "body: " + body);
        assertTrue(body.matches(".*\"B\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+\",\"lastDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+\",\"quantity\":2}.*"), () -> "body: " + body);

        taskCaptor.getValue().run();
        verifyNoMoreInteractions(eventRouter);
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    @Test
    void testLogAndcollect() throws IOException {
        RuntimeException exception = new RuntimeException("test-message");
        errorCollector.collect(logger, "A", exception);
        verify(logger).error(eq("A"), same(exception));

        verifyNoMoreInteractions(logger);
        verifyNoMoreInteractions(eventRouter);

        taskCaptor.getValue().run();

        ArgumentCaptor<EventBatch> eventBatchCaptor = ArgumentCaptor.forClass(EventBatch.class);
        verify(eventRouter).sendAll(eventBatchCaptor.capture());

        assertEquals(1, eventBatchCaptor.getValue().getEventsCount());
        Event event = eventBatchCaptor.getValue().getEvents(0);

        assertEquals("mstore internal problem(s): 1", event.getName());
        assertEquals("InternalError", event.getType());
        assertEquals(EventStatus.FAILED, event.getStatus());

        String body = event.getBody().toStringUtf8();
        assertTrue(body.matches("\\[\\{\"errors\":\\{\"A\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+\",\"quantity\":1}}}]"), () -> "body: " + body);

        taskCaptor.getValue().run();
        verifyNoMoreInteractions(eventRouter);
    }

    @Test
    void testClose() throws Exception {
        errorCollector.collect("A");
        verifyNoMoreInteractions(eventRouter);

        errorCollector.close();

        verify(future).cancel(eq(true));

        ArgumentCaptor<EventBatch> eventBatchCaptor = ArgumentCaptor.forClass(EventBatch.class);
        verify(eventRouter).sendAll(eventBatchCaptor.capture());

        assertEquals(1, eventBatchCaptor.getValue().getEventsCount());
        Event event = eventBatchCaptor.getValue().getEvents(0);

        assertEquals("mstore internal problem(s): 1", event.getName());
        assertEquals("InternalError", event.getType());
        assertEquals(EventStatus.FAILED, event.getStatus());

        String body = event.getBody().toStringUtf8();
        assertTrue(body.matches("\\[\\{\"errors\":\\{\"A\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+\",\"quantity\":1}}}]"), () -> "body: " + body);

        verifyNoMoreInteractions(eventRouter);
    }
}