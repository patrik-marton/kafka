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

import com.cloudera.dim.kafka.connect.transforms.ConvertFromBytes.Key;
import com.cloudera.dim.kafka.connect.transforms.ConvertFromBytes.Value;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConvertFromBytesTest extends ConvertAbstractTest {
    @Test
    public void testKeyConversion() {
        ConnectHeaders connectHeaders = createConnectHeaders();
        setupHeaderConverterMock();

        SchemaAndValue convertedKey = new SchemaAndValue(Schema.INT64_SCHEMA, 12345L);
        when(mockConverter.toConnectData(any(), any(), any())).thenReturn(convertedKey);

        byte[] keyBlob = {3};
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.BYTES_SCHEMA, keyBlob,
                Schema.FLOAT64_SCHEMA, 123.45, 1L, 6789L, TimestampType.CREATE_TIME, connectHeaders);

        Key<SinkRecord> transformation = new Key<>(mockConverter, mockHeaderConverter);
        assertTrue(transformation.isKey());

        SinkRecord result = transformation.apply(record);

        // Assert key was converted
        assertEquals(convertedKey.schema(), result.keySchema());
        assertEquals(convertedKey.value(), result.key());
        // Assert value is unchanged
        assertSame(record.value(), result.value());
        assertEquals(record.valueSchema(), result.valueSchema());

        verifyConverterInteractions(keyBlob);
        verifyHeaderConverterInteractions();
    }

    @Test
    public void testValueConversion() {
        ConnectHeaders connectHeaders = createConnectHeaders();
        setupHeaderConverterMock();

        SchemaAndValue convertedValue = new SchemaAndValue(Schema.INT64_SCHEMA, 12345L);
        when(mockConverter.toConnectData(any(), any(), any())).thenReturn(convertedValue);

        byte[] valueBlob = {3};
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.FLOAT64_SCHEMA, 123.45,
                Schema.BYTES_SCHEMA, valueBlob, 1L, 6789L, TimestampType.CREATE_TIME, connectHeaders);

        Value<SinkRecord> transformation = new Value<>(mockConverter, mockHeaderConverter);
        assertFalse(transformation.isKey());

        SinkRecord result = transformation.apply(record);

        // Assert value was converted
        assertEquals(convertedValue.schema(), result.valueSchema());
        assertEquals(convertedValue.value(), result.value());
        // Assert key is unchanged
        assertSame(record.key(), result.key());
        assertEquals(record.keySchema(), result.keySchema());

        verifyConverterInteractions(valueBlob);
        verifyHeaderConverterInteractions();
    }

    private void verifyConverterInteractions(byte[] blob) {
        Headers headers = createKafkaHeaders();
        verify(mockConverter)
                .toConnectData(eq(TOPIC), HeadersEquals.headersEq(headers), eq(blob));
    }
}
