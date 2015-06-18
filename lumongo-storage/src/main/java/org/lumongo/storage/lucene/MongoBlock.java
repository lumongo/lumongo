package org.lumongo.storage.lucene;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

public class MongoBlock {

	protected final MongoFile mongoFile;
	protected final int blockNumber;
	protected final long blockKey;

	protected final Object lock;

	protected byte[] bytes;

	private boolean dirty;

	public MongoBlock(MongoFile mongoFile, int blockNumber, byte[] bytes) {
		this.mongoFile = mongoFile;
		this.blockNumber = blockNumber;
		this.bytes = bytes;
		this.dirty = false;
		this.blockKey = computeBlockKey(mongoFile, blockNumber);
		this.lock = new Object();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MongoBlock that = (MongoBlock) o;

		if (blockKey != that.blockKey) {
			return false;
		}

		return true;

	}

	public void markDirty() {
		synchronized (lock) {
			dirty = true;
		}
	}

	public void flushIfDirty() {
		synchronized (lock) {
			if (dirty) {
				mongoFile.storeBlock(this);
				dirty = false;
			}
		}
	}

	@Override
	public int hashCode() {
		return Long.hashCode(blockKey);
	}

	protected static long computeBlockKey(MongoFile mongoFile, int blockNumber) {
		return ((0xFFFF & (long)mongoFile.indexNumber)) << 48 | ((0xFFFF & (long) mongoFile.fileNumber)) << 32 | (blockNumber & 0xffffffffL);
	}



}
