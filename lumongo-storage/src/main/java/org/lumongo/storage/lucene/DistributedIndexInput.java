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

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class DistributedIndexInput extends IndexInput {
	private final NosqlFile nosqlFile;
	
	protected long position;
	protected long sliceOffset;
	protected long length;
	
	public DistributedIndexInput(NosqlFile nosqlFile) {
		this(nosqlFile, 0, nosqlFile.getFileLength());
	}
	
	public DistributedIndexInput(NosqlFile nosqlFile, long sliceOffset, long length) {
		super(DistributedIndexInput.class.getSimpleName() + "(" + nosqlFile.getFileName() + ")");
		this.nosqlFile = nosqlFile;
		this.sliceOffset = sliceOffset;
		this.length = length;
	}
	
	@Override
	public void close() throws IOException {
		//NO-OP
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
	public long length() {
		return length;
	}
	
	@Override
	public byte readByte() throws IOException {
		return nosqlFile.readByte(position++ + sliceOffset);
	}
	
	@Override
	public void readBytes(byte[] b, int offset, int length) throws IOException {
		nosqlFile.readBytes(position + sliceOffset, b, offset, length);
		position += length;
	}
	
	@Override
	public IndexInput slice(String sliceDescription, final long sliceOffset, final long length) throws IOException {
		final DistributedIndexInput dii = new DistributedIndexInput(nosqlFile, this.sliceOffset + sliceOffset, length);
		return dii;
	}
	
	@Override
	public IndexInput clone() {
		IndexInput ii = new DistributedIndexInput(nosqlFile, sliceOffset, length);
		try {
			ii.seek(getFilePointer());
		}
		catch (IOException ioe) {
			throw new AssertionError(ioe);
		}
		
		return ii;
	}
}