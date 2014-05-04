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

public interface NosqlFile {
	
	public int getFileNumber();
	
	public String getFileName();
	
	public long getFileLength();
	
	public void setFileLength(long fileLength);
	
	public long getLastModified();
	
	public void setLastModified(long currentTime);
	
	public void write(long position, byte b) throws IOException;
	
	public void write(long position, byte[] b, int offset, int length) throws IOException;
	
	public void flush() throws IOException;
	
	public byte readByte(long position) throws IOException;
	
	public void readBytes(long position, byte[] b, int offset, int length) throws IOException;
	
	public int getBlockSize();
	
	public boolean isCompressed();
	
	public long getChecksum();
	
	public void resetChecksum();
	
}