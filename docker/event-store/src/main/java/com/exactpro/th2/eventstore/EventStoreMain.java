/******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2.eventstore;

import com.exactpro.th2.store.common.Configuration;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.exactpro.th2.store.common.Configuration.readConfiguration;

public class EventStoreMain {

    private final static Logger LOGGER = LoggerFactory.getLogger(EventStoreMain.class);

    public static void main(String[] args) {
        try {
            Configuration configuration = readConfiguration(args);
            Vertx vertx = Vertx.vertx();
            EventStoreVerticle eventStoreVerticle = new EventStoreVerticle(configuration);
            vertx.deployVerticle(eventStoreVerticle);
            LOGGER.info("event store started on {} port", configuration.getPort());
        } catch (Exception e) {
            LOGGER.error("fatal error: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }
}
