package org.apache.lucene.analysis.lsh;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.io.Reader;

public class LSH {

	public static Query createSlowQuery(Analyzer analyzer, String field, Reader reader, int numHash, float sim) throws IOException {
		BooleanQuery.Builder query = new BooleanQuery.Builder();

		try (TokenStream ts = analyzer.tokenStream(field, reader)) {
			CharTermAttribute termAttr = ts.getAttribute(CharTermAttribute.class);
			ts.reset();
			while (ts.incrementToken()) {
				query.add(new TermQuery(new Term(field, termAttr.toString())), BooleanClause.Occur.SHOULD);
			}
			ts.end();
		}
		query.setMinimumNumberShouldMatch((int) (sim * numHash));
		return query.build();
	}

	public static Analyzer createMinHashAnalyzer(final Tokenizer tokenizer, final int numHash) {
		return new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				final TokenStream stream = new MinHashFilter(tokenizer, numHash);
				return new TokenStreamComponents(tokenizer, stream);
			}
		};
	}

}