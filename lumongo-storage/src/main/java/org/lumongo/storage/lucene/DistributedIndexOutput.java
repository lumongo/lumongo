package org.lumongo.storage.lucene;

/**
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

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

public class DistributedIndexOutput extends IndexOutput {
	
	private final NosqlFile nosqlFile;
	
	private boolean isOpen;
	private long position;
	
	public DistributedIndexOutput(NosqlFile nosqlFile) throws IOException {
		this.nosqlFile = nosqlFile;
		this.isOpen = true;
	}
	
	@Override
	public void close() throws IOException {
		if (isOpen) {
			flush();
			isOpen = false;
		}
	}
	
	@Override
	public long length() throws IOException {
		return nosqlFile.getFileLength();
	}
	
	@Override
	public void flush() throws IOException {
		nosqlFile.flush();
	}
	
	@Override
	public long getFilePointer() {
		return position;
	}
	
	@Override
	public void seek(long pos) throws IOException {
		this.position = pos;
	}
	
	@Override
	public void writeByte(byte b) throws IOException {
		nosqlFile.write(position, b);
		position += 1;
	}
	
	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {
		nosqlFile.write(position, b, offset, length);
		position += length;
	}
	
	@Override
	public void setLength(long length) throws IOException {
		nosqlFile.setFileLength(length);
	}
	
	@Override
	public long getChecksum() throws IOException {
		flush();
		return nosqlFile.getChecksum();
	}
	
}