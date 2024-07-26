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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Transformation responsible for deserializing binary data into a Connect record using a {@link Converter} and {@link Converter#toConnectData(String, Headers, byte[])}.
 * The converter can be configured using the {@link ConvertConfig#CONVERTER_CLASS_KEY} config, and converter configs
 * can be provided using the {@link ConvertConfig#CONVERTER_CONFIG_PREFIX} prefix.
 * This transformation also has to convert the Connect record headers to binary which is done with a {@link HeaderConverter}.
 * The header converter can be configured using the {@link ConvertConfig#HEADER_CONVERTER_CLASS_KEY} config, and header
 * converter configs can be provided using the {@link ConvertConfig#HEADER_CONVERTER_CONFIG_PREFIX} prefix.
 */
public abstract class ConvertFromBytes<R extends ConnectRecord<R>> implements Transformation<R> {
    private Converter converter;
    private HeaderConverter headerConverter;

    public ConvertFromBytes() {
    }

    ConvertFromBytes(Converter converter, HeaderConverter headerConverter) {
        this.converter = converter;
        this.headerConverter = headerConverter;
    }

    @Override
    public ConfigDef config() {
        return ConvertConfig.CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> map) {
        ConvertConfig config = new ConvertConfig(map);
        converter = config.createConverter(isKey());
        headerConverter = config.createHeaderConverter();
    }

    @Override
    public void close() {
        Utils.tryClose(converter, "ConvertFromBytes converter");
        Utils.tryClose(headerConverter, "ConvertFromBytes header converter");
    }

    @Override
    public R apply(R record) {
        try {
            Headers convertedHeaders = Utils.convertHeaders(record, headerConverter);
            byte[] binaryValue = extractBinaryValue(record);
            SchemaAndValue result = converter.toConnectData(record.topic(), convertedHeaders, binaryValue);
            return newRecord(record, result);
        } catch (Exception e) {
            throw new DataException("Failed to convert record due to converter error", e);
        }
    }

    protected abstract boolean isKey();

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, SchemaAndValue updatedValue);

    private byte[] extractBinaryValue(R record) {
        Schema schema = operatingSchema(record);
        Object value = operatingValue(record);
        if (schema == null || Type.BYTES.equals(schema.type())) {
            if (value == null) {
                return null;
            } else if (value instanceof byte[]) {
                return (byte[]) value;
            } else if (value instanceof ByteBuffer) {
                return ((ByteBuffer) value).array();
            }
        }
        throw new DataException("Data must be in binary format");
    }

    public static class Key<R extends ConnectRecord<R>> extends ConvertFromBytes<R> {

        public Key() {
        }

        //Visible for testing
        Key(Converter converter, HeaderConverter headerConverter) {
            super(converter, headerConverter);
        }

        @Override
        protected boolean isKey() {
            return true;
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, SchemaAndValue updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedValue.schema(), updatedValue.value(),
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ConvertFromBytes<R> {

        public Value() {
        }

        //Visible for testing
        Value(Converter converter, HeaderConverter headerConverter) {
            super(converter, headerConverter);
        }

        @Override
        protected boolean isKey() {
            return false;
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, SchemaAndValue updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedValue.schema(), updatedValue.value(), record.timestamp());
        }
    }
}
