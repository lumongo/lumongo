/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lumongo.server.search.facet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * Default implementation of {@link LumongoSortedSetDocValuesFacetCounts}
 */
public class LumongoSortedSetDocValuesReaderState extends SortedSetDocValuesReaderState {

    private final String field;
    private final int valueCount;

    /** {@link IndexReader} passed to the constructor. */
    public final IndexReader origReader;

    private OrdinalMap ordinalMap;

    private OrdRange ordRange;

    /** Creates this, pulling doc values from the default {@link
     *  FacetsConfig#DEFAULT_INDEX_FIELD_NAME}. */
    public LumongoSortedSetDocValuesReaderState(IndexReader reader) throws IOException {
        this(reader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
    }

    /** Creates this, pulling doc values from the specified
     *  field. */
    public LumongoSortedSetDocValuesReaderState(IndexReader reader, String field) throws IOException {
        this.field = field;
        this.origReader = reader;

        // We need this to create thread-safe MultiSortedSetDV
        // per collector:
        SortedSetDocValues dv = getDocValues();
        if (dv == null) {
            throw new IllegalArgumentException("field \"" + field + "\" was not indexed with SortedSetDocValues");
        }
        if (dv.getValueCount() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("can only handle valueCount < Integer.MAX_VALUE; got " + dv.getValueCount());
        }
        valueCount = (int) dv.getValueCount();

        ordRange = new OrdRange(0, valueCount-1);
    }

    /** Return top-level doc values. */
    @Override
    public SortedSetDocValues getDocValues() throws IOException {
        // TODO: this is dup'd from slow composite reader wrapper ... can we factor it out to share?

        synchronized (this) {

            if (ordinalMap == null) {
                // uncached, or not a multi dv
                SortedSetDocValues dv = MultiDocValues.getSortedSetValues(origReader, field);
                if (dv instanceof MultiSortedSetDocValues) {
                    ordinalMap = ((MultiSortedSetDocValues)dv).mapping;
                }
                return dv;
            }
        }

        int size = origReader.leaves().size();
        final SortedSetDocValues[] values = new SortedSetDocValues[size];
        final int[] starts = new int[size+1];
        for (int i = 0; i < size; i++) {
            LeafReaderContext context = origReader.leaves().get(i);
            final LeafReader reader = context.reader();
            final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
            if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED_SET) {
                return null;
            }
            SortedSetDocValues v = reader.getSortedSetDocValues(field);
            if (v == null) {
                v = DocValues.emptySortedSet();
            }
            values[i] = v;
            starts[i] = context.docBase;
        }
        starts[size] = origReader.maxDoc();
        return new MultiSortedSetDocValues(values, starts, ordinalMap);
    }

    /** Returns mapping from prefix to {@link OrdRange}. */
    @Override
    public Map<String,OrdRange> getPrefixToOrdRange() {
        HashMap<String, OrdRange> map = new HashMap<>();
        map.put(field, ordRange);
        return map;
    }

    /** Returns the {@link OrdRange} for this dimension. */
    @Override
    public OrdRange getOrdRange(String dim) {

        return ordRange;
    }

    /** Indexed field we are reading. */
    @Override
    public String getField() {
        return field;
    }

    @Override
    public IndexReader getOrigReader() {
        return origReader;
    }

    /** Number of unique labels. */
    @Override
    public int getSize() {
        return valueCount;
    }

}
