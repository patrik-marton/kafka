/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright (c) 2024 Cloudera, Inc. All rights reserved.

package com.cloudera.dim.kafka.connect.transforms;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static void tryClose(Object resource, String name) {
        if (resource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) resource).close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close resource {}", name, e);
            }
        }
    }

    public static Headers convertHeaders(ConnectRecord<?> record, HeaderConverter headerConverter) {
        HeadersImpl headers = new HeadersImpl();
        for (Header header : record.headers()) {
            try {
                byte[] value = headerConverter.fromConnectHeader(record.topic(), header.key(), header.schema(), header.value());
                headers.add(header.key(), value);
            } catch (Exception e) {
                throw new DataException("Failed to convert header " + header.key(), e);
            }
        }
        return headers;
    }
}
