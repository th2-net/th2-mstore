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

package com.exactpro.th2.messagestore;

import java.net.InetAddress;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.schema.cradle.CradleConfiguration;
import com.exactpro.th2.schema.factory.AbstractCommonFactory;
import com.exactpro.th2.schema.factory.CommonFactory;
import com.exactpro.th2.store.common.configuration.StoreConfiguration;

public class MessageStore {

    private final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private MessageBatchStore parsedStore;
    private RawMessageBatchStore rawStore;
    private CradleManager cradleManager;

    public MessageStore(AbstractCommonFactory factory) throws CradleStorageException {

        CradleConfiguration cradleConfiguration = factory.getCradleConfiguration();

        CassandraConnectionSettings cassandraConnectionSettings = new CassandraConnectionSettings(
                cradleConfiguration.getDataCenter(),
                cradleConfiguration.getHost(),
                cradleConfiguration.getPort(),
                cradleConfiguration.getKeyspace());

        if (StringUtils.isNotEmpty(cradleConfiguration.getUsername())) {
            cassandraConnectionSettings.setUsername(cradleConfiguration.getUsername());
        }

        if (StringUtils.isNotEmpty(cradleConfiguration.getPassword())) {
            cassandraConnectionSettings.setPassword(cradleConfiguration.getPassword());
        }

        this.cradleManager = new CassandraCradleManager(new CassandraConnection(cassandraConnectionSettings));


        parsedStore = new MessageBatchStore(factory.getMessageRouterParsedBatch(), cradleManager);
        rawStore = new RawMessageBatchStore(factory.getMessageRouterRawBatch(), cradleManager);
    }

    public void start() {
        try {
            String cradleInstanceName = InetAddress.getLocalHost().getHostName();
            cradleManager.init(cradleInstanceName);
            logger.info("Cradle manager init successfully with {} instance name", cradleInstanceName);
        } catch (Exception e) {
            throw new IllegalStateException("Can not create cradle manager", e);
        }

        try {
            parsedStore.start();
        } catch (Exception e) {
            throw new IllegalStateException("Can not start storage for parsed messages", e);
        }

        try {
            rawStore.start();
            logger.info("Message store start successfully");
        } catch (Exception e) {
            try {
                parsedStore.dispose();
            } finally {
                throw new IllegalStateException("Can not start storage for raw messages", e);
            }

        }
    }

    public void dispose() {
        try {
            parsedStore.dispose();
        } catch (Exception e) {
            throw new IllegalStateException("Can not dispose storage for parsed messages", e);
        }

        try {
            rawStore.dispose();
        } catch (Exception e) {
            throw new IllegalStateException("Can not dispose storage for raw messages", e);
        }
        logger.info("Storage was stopped");
    }

    public void waitShutdown() throws InterruptedException {
        synchronized (this) {
            wait();
        }
    }

    public static void main(String[] args) {
        try {
            CommonFactory factory = CommonFactory.createFromArguments(args);
            MessageStore store = new MessageStore(factory);

            Runtime.getRuntime().addShutdownHook(new Thread(store::dispose));

            store.start();
            store.waitShutdown();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error occurred. Exit the program");
            System.exit(-1);
        }
    }
}

