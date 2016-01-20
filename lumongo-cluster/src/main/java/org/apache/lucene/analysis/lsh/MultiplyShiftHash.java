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

import java.util.Random;

public class MultiplyShiftHash {
	private int a;
	private int l;

	public MultiplyShiftHash(int a, int l) {
		this.a = a;
		this.l = l;
	}

	public static MultiplyShiftHash randomInit(Random r, int numBit) {
		if (numBit > 32)
			throw new IllegalArgumentException("Numbit must not greater than 32");

		int a = r.nextInt();
		if (a % 2 == 0)
			a++;
		return new MultiplyShiftHash(a, numBit);
	}

	public int hash(int x) {
		return Math.abs(a * x >>> (32 - l));
	}
}