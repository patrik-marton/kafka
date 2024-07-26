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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.progress.ThreadSafeMockingProgress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HeadersEquals implements ArgumentMatcher<Headers> {
    private final Map<String, List<HeaderEqHelper>> expected;

    HeadersEquals(Map<String, List<HeaderEqHelper>> expected) {
        this.expected = expected;
    }

    public static Headers headersEq(Headers expected) {
        HeadersEquals matcher = new HeadersEquals(headersToMap(expected));
        ThreadSafeMockingProgress.mockingProgress().getArgumentMatcherStorage().reportMatcher(matcher);
        return new HeadersImpl();
    }

    private static Map<String, List<HeaderEqHelper>> headersToMap(Headers headers) {
        Map<String, List<HeaderEqHelper>> result = new HashMap<>();
        headers.forEach(h -> result.computeIfAbsent(h.key(), k -> new ArrayList<>())
                .add(new HeaderEqHelper(h.key(), h.value())));
        return result;
    }

    @Override
    public boolean matches(Headers headers) {
        Map<String, List<HeaderEqHelper>> actual = headersToMap(headers);
        return expected.equals(actual);
    }

    static class HeaderEqHelper implements Header {
        private final String key;
        private final byte[] value;

        HeaderEqHelper(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HeaderEqHelper that = (HeaderEqHelper) o;
            return Objects.equals(key, that.key) && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(key);
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }

        @Override
        public String toString() {
            return "HeaderEqHelper{"
                    + "key='" + key + '\''
                    + ", value=" + Arrays.toString(value)
                    + '}';
        }
    }
}
