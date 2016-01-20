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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class MinHashFilter extends TokenFilter {
	private static final int NUM_BIT_OF_BUCKETS = 20;
	private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
	private final PositionIncrementAttribute posIncrAttr = addAttribute(PositionIncrementAttribute.class);
	private int index = 0;
	private int[] result;
	private MultiplyShiftHash[] hashes;

	public MinHashFilter(TokenStream input, int numHash) {
		super(input);
		hashes = new MultiplyShiftHash[numHash];
		Random r = new Random(1000);
		for (int i = 0; i < numHash; i++) {
			hashes[i] = MultiplyShiftHash.randomInit(r, NUM_BIT_OF_BUCKETS);
		}
		result = new int[hashes.length];

	}

	@Override
	public final boolean incrementToken() throws IOException {
		if (index == -1) {
			Arrays.fill(result, Integer.MAX_VALUE);

			while (input.incrementToken()) {
				for (int i = 0; i < hashes.length; i++) {
					result[i] = Math.min(result[i], hashes[i].hash(termAttr.toString().hashCode()));
				}
			}
			index = 0;
		}

		if (index < hashes.length) {
			termAttr.setEmpty().append((index + 1) + "" + result[index++]);
			return true;
		}

		return false;
	}

	@Override
	public void end() throws IOException {
		super.end();
	}

	@Override
	public void reset() throws IOException {
		super.reset();
		index = -1;
	}
}