/*
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
 */
package com.exactpro.th2.mstore;

import static com.exactpro.th2.ConfigurationUtils.safeLoad;
import static java.lang.System.getenv;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.BooleanUtils;

import com.exactpro.th2.store.common.Configuration;

public class MStoreConfiguration extends Configuration {

    public static final String ENV_CHECK_SUBSCRIPTIONS_ON_START = "CHECK_SUBSCRIPTIONS_ON_START";

    public static Boolean isEnvCheckSubscriptionsOnStart() {
        return BooleanUtils.toBoolean(getenv(ENV_CHECK_SUBSCRIPTIONS_ON_START));
    }

    /**
     * Box should shutdown in case any subscription failure
     */
    private boolean checkSubscriptionsOnStart = isEnvCheckSubscriptionsOnStart();

    public boolean isCheckSubscriptionsOnStart() {
        return checkSubscriptionsOnStart;
    }

    public void setCheckSubscriptionsOnStart(boolean checkSubscriptionsOnStart) {
        this.checkSubscriptionsOnStart = checkSubscriptionsOnStart;
    }

    public static MStoreConfiguration load(InputStream inputStream) throws IOException {
        return YAML_READER.readValue(inputStream, MStoreConfiguration.class);
    }

    public static MStoreConfiguration readConfiguration(String[] args) {
        if (args.length > 0) {
            return safeLoad(MStoreConfiguration::load, MStoreConfiguration::new, args[0]);
        }
        return new MStoreConfiguration();
    }
}
