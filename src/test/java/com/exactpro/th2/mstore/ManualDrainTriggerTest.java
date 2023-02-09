/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ManualDrainTriggerTest {

    private final ExecutorService executor = mock(ExecutorService.class);


    private void setMessages(ManualDrainTrigger trigger, int count) {
        for (int i = 0; i < count; i++) {
            trigger.registerMessage();
        }
    }

    @Test
    public void testMessageRegistration() {
        ManualDrainTrigger trigger = new ManualDrainTrigger(executor, 10);

        for (int i = 1; i < 10; i++) {
            trigger.registerMessage();
            assertEquals(i, trigger.getCounter());
        }
    }

    @Test
    public void testMessageUnregistration() {
        ManualDrainTrigger trigger = new ManualDrainTrigger(executor, 10);

        final int registrations = 10;
        final int unregistrations = 3;

        setMessages(trigger, registrations);
        trigger.unregisterMessages(unregistrations);

        assertEquals(registrations - unregistrations, trigger.getCounter());
    }

    @Test
    public void drainingNotTriggeredWhenThresholdIsZero() {
        final int registrations = 10;
        ManualDrainTrigger trigger = new ManualDrainTrigger(executor, 0);

        setMessages(trigger, registrations);

        assertFalse(trigger.isDrainTriggered());
        trigger.runConditionally(() -> {});
        assertFalse(trigger.isDrainTriggered());

        verify(executor, never()).submit(any(Runnable.class));
    }

    @Test
    public void drainingNotTriggeredBeforeThreshold() {
        final int registrations = 10;
        ManualDrainTrigger trigger = new ManualDrainTrigger(executor, registrations + 1);

        setMessages(trigger, registrations);

        assertFalse(trigger.isDrainTriggered());
        trigger.runConditionally(() -> {});
        assertFalse(trigger.isDrainTriggered());

        verify(executor, never()).submit(any(Runnable.class));
    }

    @Test
    public void drainingIsTriggered() {
        final int registrations = 10;
        ManualDrainTrigger trigger = new ManualDrainTrigger(executor, registrations - 1);

        setMessages(trigger, registrations);

        assertFalse(trigger.isDrainTriggered());
        trigger.runConditionally(() -> {});
        assertTrue(trigger.isDrainTriggered());

        verify(executor, times(1)).submit(any(Runnable.class));
    }

    @Test
    public void drainingNotTriggeredWhileStillDraining() {
        final int registrations = 10;
        ManualDrainTrigger trigger = new ManualDrainTrigger(executor, registrations - 1);

        setMessages(trigger, registrations);

        assertFalse(trigger.isDrainTriggered());
        trigger.runConditionally(() -> {});
        assertTrue(trigger.isDrainTriggered());


        setMessages(trigger, 2 * registrations);

        assertTrue(trigger.isDrainTriggered());
        trigger.runConditionally(() -> {});
        assertTrue(trigger.isDrainTriggered());

        verify(executor, times(1)).submit(any(Runnable.class));
    }

    @Test
    public void completionClearsDrainingFlag() {
        final int registrations = 10;
        ManualDrainTrigger trigger = new ManualDrainTrigger(executor, registrations - 1);

        setMessages(trigger, registrations);

        assertFalse(trigger.isDrainTriggered());
        trigger.runConditionally(() -> {});
        assertTrue(trigger.isDrainTriggered());

        trigger.completeDraining();
        assertFalse(trigger.isDrainTriggered());

        verify(executor, times(1)).submit(any(Runnable.class));
    }
}