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
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;

public class ConvertConfig extends AbstractConfig {
    public static final String CONVERTER_CLASS_KEY = "converter";
    public static final String CONVERTER_CONFIG_PREFIX = "converter.";
    public static final String CONVERTER_CLASS_DOC = "The class of the converter to use when converting the record."
            + " Make sure that the converter configuration matches the conversion format of the records."
            + " Additional configs can be passed to the converter by using the '" + CONVERTER_CONFIG_PREFIX + "' prefix.";
    public static final String HEADER_CONVERTER_CLASS_KEY = "header.converter";
    public static final String HEADER_CONVERTER_CONFIG_PREFIX = "header.converter.";
    public static final String HEADER_CONVERTER_CLASS_DEFAULT = SimpleHeaderConverter.class.getName();
    public static final String HEADER_CONVERTER_CLASS_DOC = "The class of the header converter to use when converting the record."
            + " Make sure that the header converter configuration matches the conversion format of the record headers."
            + " Additional configs can be passed to the header converter by using the '" + HEADER_CONVERTER_CONFIG_PREFIX + "' prefix.";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONVERTER_CLASS_KEY, Type.CLASS, Importance.HIGH, CONVERTER_CLASS_DOC)
            .define(HEADER_CONVERTER_CLASS_KEY, Type.CLASS, HEADER_CONVERTER_CLASS_DEFAULT,
                    Importance.MEDIUM, HEADER_CONVERTER_CLASS_DOC);

    public ConvertConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public Converter createConverter(boolean key) {
        Class<?> converterClass = getClass(CONVERTER_CLASS_KEY);
        Converter converter = (Converter) org.apache.kafka.common.utils.Utils.newInstance(converterClass);
        converter.configure(configsWithPrefixStripped(CONVERTER_CONFIG_PREFIX), key);
        return converter;
    }

    public HeaderConverter createHeaderConverter() {
        return getConfiguredInstance(HEADER_CONVERTER_CLASS_KEY, HeaderConverter.class,
                configsWithPrefixStripped(HEADER_CONVERTER_CONFIG_PREFIX));
    }

    private Map<String, Object> configsWithPrefixStripped(String prefix) {
        return originals()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(e -> e.getKey().substring(prefix.length()), Entry::getValue));
    }
}
