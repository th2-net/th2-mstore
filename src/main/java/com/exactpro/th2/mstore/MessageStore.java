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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.exactpro.th2.common.metrics.CommonMetrics.LIVENESS_MONITOR;
import static com.exactpro.th2.common.metrics.CommonMetrics.READINESS_MONITOR;
import static com.exactpro.th2.common.utils.ExecutorServiceUtilsKt.shutdownGracefully;

public class MessageStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageStore.class);
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("error-collector-%d").build();

    public static void main(String[] args) {

        ShutdownManager shutdownManager = new ShutdownManager();
        try {
            LIVENESS_MONITOR.enable();
            shutdownManager.register();

            // Load configuration
            CommonFactory factory = CommonFactory.createFromArguments(args);
            shutdownManager.registerResource(factory);

            Configuration config = factory.getCustomConfiguration(Configuration.class);
            if (config == null) {
                config = Configuration.createDefault();
            }

            ObjectMapper mapper = new ObjectMapper();
            LOGGER.info("Effective configuration:\n{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config));

            // Initialize Cradle
            CradleManager cradleManager = factory.getCradleManager();
            shutdownManager.registerResource(cradleManager);
            CradleStorage storage = cradleManager.getStorage();

            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
            shutdownManager.registerResource(() -> shutdownGracefully(executor, 5, TimeUnit.SECONDS));

            ErrorCollector errorCollector = new ErrorCollector(executor, factory.getEventBatchRouter(), factory.getRootEventId());
            shutdownManager.registerResource(errorCollector);

            // Initialize persistor
            MessagePersistor persistor = new MessagePersistor(errorCollector, storage, config);
            shutdownManager.registerResource(persistor);

            // Initialize processors
            AbstractMessageProcessor protoProcessor = new ProtoRawMessageProcessor(
                    errorCollector, factory.getMessageRouterRawBatch(),
                    storage,
                    persistor,
                    config,
                    factory.getConnectionManagerConfiguration().getPrefetchCount());
            shutdownManager.registerResource(protoProcessor);

            AbstractMessageProcessor transportProcessor = new TransportGroupProcessor(
                    errorCollector, factory.getTransportGroupBatchRouter(),
                    storage,
                    persistor,
                    config,
                    factory.getConnectionManagerConfiguration().getPrefetchCount());
            shutdownManager.registerResource(transportProcessor);

            persistor.start();
            protoProcessor.start();
            transportProcessor.start();

            READINESS_MONITOR.enable();
            LOGGER.info("mstore started");

            shutdownManager.awaitShutdown();

        } catch (InterruptedException e) {
            LOGGER.info("The main thread has been interrupted", e);
        } catch (Exception e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            shutdownManager.closeResources();
            System.exit(1);
        }
        LOGGER.info("mstore stopped");
    }
}