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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Transformation responsible for serializing a Connect record using a {@link Converter} and {@link Converter#fromConnectData(String, Headers, Schema, Object)}.
 * The converter can be configured using the {@link ConvertConfig#CONVERTER_CLASS_KEY} config, and converter configs
 * can be provided using the {@link ConvertConfig#CONVERTER_CONFIG_PREFIX} prefix.
 * This transformation also has to convert the Connect record headers to binary which is done with a {@link HeaderConverter}.
 * The header converter can be configured using the {@link ConvertConfig#HEADER_CONVERTER_CLASS_KEY} config, and header
 * converter configs can be provided using the {@link ConvertConfig#HEADER_CONVERTER_CONFIG_PREFIX} prefix.
 */
public abstract class ConvertToBytes<R extends ConnectRecord<R>> implements Transformation<R> {
    private Converter converter;
    private HeaderConverter headerConverter;

    public ConvertToBytes() {
    }

    ConvertToBytes(Converter converter, HeaderConverter headerConverter) {
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
        Utils.tryClose(converter, "ConvertToBytes converter");
        Utils.tryClose(headerConverter, "ConvertToBytes header converter");
    }

    @Override
    public R apply(R record) {
        Schema schema = operatingSchema(record);
        Object value = operatingValue(record);
        try {
            Headers convertedHeaders = Utils.convertHeaders(record, headerConverter);
            byte[] recordValue = converter.fromConnectData(record.topic(), convertedHeaders, schema, value);
            return newRecord(record, recordValue);
        } catch (Exception e) {
            throw new DataException("Failed to convert record due to converter error", e);
        }
    }

    protected abstract boolean isKey();

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ConvertToBytes<R> {

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
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), Schema.OPTIONAL_BYTES_SCHEMA,
                    updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ConvertToBytes<R> {

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
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    Schema.OPTIONAL_BYTES_SCHEMA, updatedValue, record.timestamp());
        }
    }
}
