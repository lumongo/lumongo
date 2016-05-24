package org.lumongo.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

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

public class ConstantSimilarity extends TFIDFSimilarity {

	@Override
	public float coord(int overlap, int maxOverlap) {
		return 1f;
	}

	@Override
	public float queryNorm(float sumOfSquaredWeights) {
		return 1f;
	}

	@Override
	public float tf(float freq) {
		return 1f;
	}

	@Override
	public float idf(long docFreq, long docCount) {
		return 1f;
	}

	@Override
	public float lengthNorm(FieldInvertState state) {
		return 1f;
	}

	@Override
	public float decodeNormValue(long norm) {
		return 1f;
	}

	@Override
	public long encodeNormValue(float f) {
		return SmallFloat.floatToByte315(f);
	}

	@Override
	public float sloppyFreq(int distance) {
		return 1f;
	}

	@Override
	public float scorePayload(int doc, int start, int end, BytesRef payload) {
		return 1f;
	}
}