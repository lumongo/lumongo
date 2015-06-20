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

	String getFileName();

	void setFileName(String fileName);

	short getFileNumber();

	long getFileLength();

	void setFileLength(long fileLength);

	long getLastModified();

	void setLastModified(long currentTime);

	void write(long position, byte b) throws IOException;

	void write(long position, byte[] b, int offset, int length) throws IOException;

	void flush() throws IOException;

	byte readByte(long position) throws IOException;

	void readBytes(long position, byte[] b, int offset, int length) throws IOException;

	int getBlockSize();

	long getChecksum();

	void resetChecksum();

	void close();
}