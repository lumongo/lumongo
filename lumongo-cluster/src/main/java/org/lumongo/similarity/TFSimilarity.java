package org.lumongo.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

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

public class TFSimilarity extends BooleanSimilarity {

	private static final Similarity BM25_SIM = new BM25Similarity();

	/** Sole constructor */
	public TFSimilarity() {
	}

	@Override
	public long computeNorm(FieldInvertState state) {
		return BM25_SIM.computeNorm(state);
	}

	@Override
	public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
		return new TFSimilarity.BooleanWeight(boost);
	}

	private static class BooleanWeight extends SimWeight {
		final float boost;

		BooleanWeight(float boost) {
			this.boost = boost;
		}
	}

	@Override
	public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
		final float boost = ((TFSimilarity.BooleanWeight) weight).boost;

		return new SimScorer() {

			@Override
			public float score(int doc, float freq) throws IOException {
				return freq * boost;
			}

			@Override
			public Explanation explain(int doc, Explanation freq) throws IOException {
				Explanation queryBoostExpl = Explanation.match(boost, "query boost");
				return Explanation
						.match(queryBoostExpl.getValue(), "score(" + getClass().getSimpleName() + ", doc=" + doc + "), computed from:", queryBoostExpl);
			}

			@Override
			public float computeSlopFactor(int distance) {
				return 1f;
			}

			@Override
			public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
				return 1f;
			}
		};
	}

}