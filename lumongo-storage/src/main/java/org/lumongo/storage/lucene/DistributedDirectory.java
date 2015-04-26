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

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

public class DistributedDirectory extends BaseDirectory {

	protected NosqlDirectory nosqlDirectory;

	public DistributedDirectory(NosqlDirectory nosqlDirectory) throws IOException {
		this(nosqlDirectory, new SingleInstanceLockFactory());
	}

	public DistributedDirectory(NosqlDirectory nosqlDirectory, LockFactory lockFactory) throws IOException {
		super(lockFactory);
		this.nosqlDirectory = nosqlDirectory;
	}

	/**
	 * ignore IOContext
	 */
	@Override
	public IndexOutput createOutput(String name, IOContext context) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(name, true);
		return new DistributedIndexOutput(nosqlFile);
	}

	@Override
	public void sync(Collection<String> names) throws IOException {
		for (String name : names) {
			NosqlFile nosqlFile = nosqlDirectory.getFileHandle(name, true);
			nosqlFile.flush();
		}

	}

	@Override
	public void renameFile(String source, String dest) throws IOException {
		ensureOpen();
		nosqlDirectory.rename(source, dest);
	}

	/**
	 * ignore IOContext
	 */
	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(name);
		return new DistributedIndexInput(nosqlFile.getFileName(), nosqlFile);
	}

	@Override
	public String[] listAll() throws IOException {
		ensureOpen();
		return nosqlDirectory.getFileNames();
	}

	@Override
	public long fileLength(String fileName) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(fileName);
		return nosqlFile.getFileLength();
	}

	@Override
	public void deleteFile(String fileName) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(fileName);
		nosqlDirectory.deleteFile(nosqlFile);
	}

	public void copyToFSDirectory(Path path) throws IOException {
		copyToDirectory(FSDirectory.open(path));
	}

	public void copyToDirectory(Directory directory) throws IOException {
		for (String file : this.listAll()) {
			directory.copyFrom(this, file, file, IOContext.DEFAULT);
		}
	}

	@Override
	public void close() throws IOException {
		isOpen = false;
		nosqlDirectory.close();
	}

	@Override
	public String toString() {
		return nosqlDirectory.toString();
	}

}