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

import com.cloudera.dim.kafka.connect.transforms.ConvertToBytes.Key;
import com.cloudera.dim.kafka.connect.transforms.ConvertToBytes.Value;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConvertToBytesTest extends ConvertAbstractTest {
    @Test
    public void testKeyConversion() {
        ConnectHeaders connectHeaders = createConnectHeaders();
        setupHeaderConverterMock();

        byte[] keyBlob = {3};
        when(mockConverter.fromConnectData(any(), any(), any(), any())).thenReturn(keyBlob);

        long keyValue = 12345L;
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.INT64_SCHEMA, keyValue,
                Schema.FLOAT64_SCHEMA, 123.45, 1L, 6789L, TimestampType.CREATE_TIME, connectHeaders);

        Key<SinkRecord> transformation = new Key<>(mockConverter, mockHeaderConverter);
        assertTrue(transformation.isKey());

        SinkRecord result = transformation.apply(record);

        // Assert key was converted
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, result.keySchema());
        assertArrayEquals(keyBlob, (byte[]) result.key());
        // Assert value is unchanged
        assertSame(record.value(), result.value());
        assertEquals(record.valueSchema(), result.valueSchema());

        verifyConverterInteractions(Schema.INT64_SCHEMA, keyValue);
        verifyHeaderConverterInteractions();
    }

    @Test
    public void testValueConversion() {
        ConnectHeaders connectHeaders = createConnectHeaders();
        setupHeaderConverterMock();

        byte[] valueBlob = {3};
        when(mockConverter.fromConnectData(any(), any(), any(), any())).thenReturn(valueBlob);

        double recordValue = 123.45;
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.INT64_SCHEMA, 12345L,
                Schema.FLOAT64_SCHEMA, recordValue, 1L, 6789L, TimestampType.CREATE_TIME, connectHeaders);

        Value<SinkRecord> transformation = new Value<>(mockConverter, mockHeaderConverter);
        assertFalse(transformation.isKey());

        SinkRecord result = transformation.apply(record);

        // Assert value was converted
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, result.valueSchema());
        assertArrayEquals(valueBlob, (byte[]) result.value());
        // Assert key is unchanged
        assertSame(record.key(), result.key());
        assertEquals(record.keySchema(), result.keySchema());

        verifyConverterInteractions(Schema.FLOAT64_SCHEMA, recordValue);
        verifyHeaderConverterInteractions();
    }

    private void verifyConverterInteractions(Schema schema, Object value) {
        Headers headers = createKafkaHeaders();
        verify(mockConverter)
                .fromConnectData(eq(TOPIC), HeadersEquals.headersEq(headers), eq(schema), eq(value));
    }
}
