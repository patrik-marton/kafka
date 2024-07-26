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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.AbstractIterator;

public class HeadersImpl implements Headers {
    private final List<Header> headerEntries = new ArrayList<>();

    @Override
    public Headers add(Header header) throws IllegalStateException {
        headerEntries.add(header);
        return this;
    }

    @Override
    public Headers add(String s, byte[] bytes) throws IllegalStateException {
        return add(new HeaderImpl(s, bytes));
    }

    @Override
    public Headers remove(String s) throws IllegalStateException {
        headerEntries.removeIf(e -> Objects.equals(s, e.key()));
        return this;
    }

    @Override
    public Header lastHeader(String s) {
        Header result = null;
        for (Header item : headerEntries) {
            if (item.key().equals(s)) {
                result = item;
            }
        }
        return result;
    }

    @Override
    public Iterable<Header> headers(String s) {
        return () -> new FilterByKeyIterator(headerEntries.iterator(), s);
    }

    @Override
    public Header[] toArray() {
        return headerEntries.toArray(new Header[0]);
    }

    @Override
    public Iterator<Header> iterator() {
        return headerEntries.iterator();
    }

    private static final class HeaderImpl implements Header {
        private final String key;
        private final byte[] value;

        public HeaderImpl(String key, byte[] value) {
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
    }

    private static final class FilterByKeyIterator extends AbstractIterator<Header> {

        private final Iterator<Header> original;
        private final String key;

        private FilterByKeyIterator(Iterator<Header> original, String key) {
            this.original = original;
            this.key = key;
        }

        protected Header makeNext() {
            while (true) {
                if (original.hasNext()) {
                    Header header = original.next();
                    if (!header.key().equals(key)) {
                        continue;
                    }
                    return header;
                }
                return this.allDone();
            }
        }
    }
}
