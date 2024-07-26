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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

abstract class ConvertAbstractTest {
    protected static final String TOPIC = "test_topic";
    protected static final String HEADER1 = "header1";
    protected static final String HEADER2 = "header2";
    protected static final boolean HEADER1_VALUE = true;
    protected static final int HEADER2_VALUE = 1;
    protected static final byte[] HEADER1_BLOB = {1};
    protected static final byte[] HEADER2_BLOB = {2};

    protected Converter mockConverter;
    protected HeaderConverter mockHeaderConverter;

    @BeforeEach
    void prepMocks() {
        mockConverter = mock(Converter.class);
        mockHeaderConverter = mock(HeaderConverter.class);
    }

    protected static ConnectHeaders createConnectHeaders() {
        ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.add(HEADER1, HEADER1_VALUE, Schema.BOOLEAN_SCHEMA);
        connectHeaders.add(HEADER2, HEADER2_VALUE, Schema.INT32_SCHEMA);
        return connectHeaders;
    }

    protected static Headers createKafkaHeaders() {
        Headers headers = new HeadersImpl();
        headers.add(HEADER1, HEADER1_BLOB);
        headers.add(HEADER2, HEADER2_BLOB);
        return headers;
    }

    protected void setupHeaderConverterMock() {
        when(mockHeaderConverter.fromConnectHeader(any(), eq(HEADER1), any(), any())).thenReturn(HEADER1_BLOB);
        when(mockHeaderConverter.fromConnectHeader(any(), eq(HEADER2), any(), any())).thenReturn(HEADER2_BLOB);
    }

    protected void verifyHeaderConverterInteractions() {
        verify(mockHeaderConverter)
                .fromConnectHeader(eq(TOPIC), eq(HEADER1), eq(Schema.BOOLEAN_SCHEMA), eq(HEADER1_VALUE));
        verify(mockHeaderConverter)
                .fromConnectHeader(eq(TOPIC), eq(HEADER2), eq(Schema.INT32_SCHEMA), eq(HEADER2_VALUE));
    }
}
