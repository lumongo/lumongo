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
	
	private MongoFile mongoFile;
	private final int fileNumber;
	private final int blockNumber;
	private final byte[] bytes;
	
	private boolean dirty;
	private String indexName;
	
	public MongoBlock(MongoFile mongoFile, int fileNumber, int blockNumber, byte[] bytes) {
		this.mongoFile = mongoFile;
		this.indexName = mongoFile.getMongoDirectory().getIndexName();
		this.fileNumber = fileNumber;
		this.blockNumber = blockNumber;
		this.bytes = bytes;
		dirty = false;
	}
	
	public MongoFile getMongoFile() {
		return mongoFile;
	}
	
	public boolean isDirty() {
		return dirty;
	}
	
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	public int getFileNumber() {
		return fileNumber;
	}
	
	public int getBlockNumber() {
		return blockNumber;
	}
	
	public byte[] getBytes() {
		return bytes;
	}
	
	public byte getByte(int blockOffset) {
		return bytes[blockOffset];
	}
	
	public void setByte(int blockOffset, byte b) {
		bytes[blockOffset] = b;
		dirty = true;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + blockNumber;
		result = prime * result + fileNumber;
		result = prime * result + indexName.hashCode();
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MongoBlock other = (MongoBlock) obj;
		if (blockNumber != other.blockNumber)
			return false;
		if (fileNumber != other.fileNumber)
			return false;
		if (!indexName.equals(other.indexName))
			return false;
		return true;
	}
	
}
