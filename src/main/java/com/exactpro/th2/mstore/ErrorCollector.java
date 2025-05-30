/*
 * Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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

import org.slf4j.Logger;

@SuppressWarnings("unused")
public interface ErrorCollector extends AutoCloseable {
    /**
     * Log error and call the {@link #collect(String)}} method
     * @param error is used as key identifier. Avoid put a lot of unique values
     */
    void collect(Logger logger, String error, Throwable cause);

    /**
     * @param error is used as key identifier. Avoid put a lot of unique values
     */
    void collect(String error);
}
