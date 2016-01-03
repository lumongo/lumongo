package org.lumongo.storage.lucene;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.lumongo.storage.constants.MongoConstants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

public class MongoDirectory implements NosqlDirectory {

	public static final String BLOCK_SIZE = "blockSize";
	public static final String BLOCK_NUMBER = "blockNumber";
	public static final String LAST_MODIFIED = "lastModified";
	public static final String LENGTH = "length";
	public static final String FILE_NAME = "fileName";
	public static final String FILE_NUMBER = "fileNumber";

	public static String BYTES = "bytes";

	public static final String FILES_SUFFIX = ".files";
	public static final String BLOCKS_SUFFIX = ".blocks";

	public static final int DEFAULT_BLOCK_SIZE = 1024 * 128;
	public static final int DEFAULT_BLOCK_MAX = 12500;

	private static short indexCount = 0;
	private static final ConcurrentHashMap<String, Short> indexNameToNumberMap = new ConcurrentHashMap<>();

	private final MongoClient mongo;
	private final String dbname;
	protected final String indexName;
	private final int blockSize;
	protected final short indexNumber;
	private final ConcurrentHashMap<String, MongoFile> nameToFileMap;

	public static void setMaxIndexBlocks(int blocks) {
		MongoFile.setMaxIndexBlocks(blocks);
	}

	/**
	 * Removes an index from a database
	 * @param mongo
	 * @param dbname
	 * @param indexName
	 */
	public static void dropIndex(MongoClient mongo, String dbname, String indexName) {
		MongoDatabase db = mongo.getDatabase(dbname);
		db.getCollection(indexName + MongoDirectory.BLOCKS_SUFFIX).drop();
		db.getCollection(indexName + MongoDirectory.FILES_SUFFIX).drop();
	}

	public MongoDirectory(MongoClient mongo, String dbname, String indexName) throws MongoException, IOException {
		this(mongo, dbname, indexName, false);
	}

	public MongoDirectory(MongoClient mongo, String dbname, String indexName, boolean sharded) throws MongoException, IOException {
		this(mongo, dbname, indexName, sharded, DEFAULT_BLOCK_SIZE);
	}

	public MongoDirectory(MongoClient mongo, String ddName, String indexName, boolean sharded, int blockSize)
					throws MongoException, IOException {

		this.mongo = mongo;
		this.dbname = ddName;
		this.indexName = indexName;
		this.blockSize = blockSize;

		synchronized (MongoDirectory.class) {
			//get back a index number to use instead of the string
			//this is not a persisted number and is just in memory
			String key = dbname + "-" + indexName;
			Short indexNumber = indexNameToNumberMap.get(key);
			if (indexNumber == null) {
				indexNameToNumberMap.put(key, indexCount);
				indexNumber = indexCount;
				indexCount++;
			}
			this.indexNumber = indexNumber;
		}

		getFilesCollection().createIndex(new Document(FILE_NUMBER, 1));

		Document indexes = new Document();
		indexes.put(FILE_NUMBER, 1);
		indexes.put(BLOCK_NUMBER, 1);
		getBlocksCollection().createIndex(indexes);

		if (sharded) {
			String blockCollectionName = getBlocksCollection().getNamespace().getFullName();
			MongoDatabase db = mongo.getDatabase(MongoConstants.StandardDBs.ADMIN);
			Document shardCommand = new Document();
			shardCommand.put(MongoConstants.Commands.SHARD_COLLECTION, blockCollectionName);
			shardCommand.put(MongoConstants.Commands.SHARD_KEY, indexes);
			db.runCommand(shardCommand);
		}

		nameToFileMap = new ConcurrentHashMap<>();

		fetchInitialContents();
	}

	public String getIndexName() {
		return indexName;
	}

	private void fetchInitialContents() throws MongoException, IOException {
		MongoCollection<Document> c = getFilesCollection();


		FindIterable<Document> cur = c.find();
		for (Document d : cur) {
			MongoFile mf = loadFileFromDBObject(d);
			nameToFileMap.put(mf.getFileName(), mf);
		}
	}

	public MongoCollection<Document> getFilesCollection() {
		MongoDatabase db = mongo.getDatabase(dbname);
		MongoCollection<Document> c = db.getCollection(indexName + FILES_SUFFIX);
		return c;
	}

	public MongoCollection<Document> getBlocksCollection() {
		MongoDatabase db = mongo.getDatabase(dbname);
		MongoCollection<Document> c = db.getCollection(indexName + BLOCKS_SUFFIX);
		return c;
	}

	@Override
	public String[] getFileNames() throws IOException {

		ConcurrentHashMap.KeySetView<String, MongoFile> strings = nameToFileMap.keySet();
		return strings.toArray(new String[strings.size()]);
	}

	@Override
	public MongoFile getFileHandle(String fileName) throws IOException {
		return getFileHandle(fileName, false);
	}

	@Override
	public MongoFile getFileHandle(String filename, boolean createIfNotFound) throws IOException {
		if (nameToFileMap.containsKey(filename)) {
			return nameToFileMap.get(filename);
		}

		MongoCollection<Document> c = getFilesCollection();

		Document query = new Document();
		query.put(FILE_NAME, filename);

		Document doc = c.find(query).first();

		if (doc != null) {
			return loadFileFromDBObject(doc);
		}
		else if (createIfNotFound) {
			return createFile(filename);
		}

		throw new FileNotFoundException(filename);

	}

	private MongoFile createFile(String fileName) throws IOException {
		synchronized (this) {

			TreeSet<Short> fileNumbers = nameToFileMap.values().stream().map(mongoFile -> mongoFile.fileNumber)
							.collect(Collectors.toCollection(TreeSet::new));

			FindIterable<Document> documents = getFilesCollection().find();
			for (Document document : documents) {
				fileNumbers.add(((Number) document.get(FILE_NUMBER)).shortValue());
			}

			Short fileNumber = null;
			for (short i = 0; i < Short.MAX_VALUE; i++) {
				if (!fileNumbers.contains(i)) {
					fileNumber = i;
					break;
				}
			}

			if (fileNumber == null) {
				throw new IOException("There are more than <" + Short.MAX_VALUE + "> files in the index");
			}

			MongoFile mongoFile = new MongoFile(this, fileName, fileNumber, blockSize);

			updateFileMetadata(mongoFile);

			nameToFileMap.putIfAbsent(mongoFile.getFileName(), mongoFile);
			return nameToFileMap.get(mongoFile.getFileName());
		}
	}

	private MongoFile loadFileFromDBObject(Document document) throws IOException {
		MongoFile mongoFile = fromDocument(document);

		nameToFileMap.putIfAbsent(mongoFile.getFileName(), mongoFile);
		return nameToFileMap.get(mongoFile.getFileName());

	}

	public MongoFile fromDocument(Document document) throws IOException {
		try {
			MongoFile mongoFile = new MongoFile(this, (String) document.get(FILE_NAME), ((Number) document.get(FILE_NUMBER)).shortValue(),
							((Number) document.get(BLOCK_SIZE)).intValue());
			mongoFile.setFileLength(((Number) document.get(LENGTH)).longValue());
			mongoFile.setLastModified(((Number) document.get(LAST_MODIFIED)).longValue());
			return mongoFile;
		}
		catch (Exception e) {
			throw new IOException("Unable to de-serialize file descriptor from: <" + document + ">: ", e);

		}
	}

	public static Document toDocument(NosqlFile nosqlFile) throws IOException {
		try {
			Document document = new Document();
			document.put(FILE_NUMBER, nosqlFile.getFileNumber());
			document.put(FILE_NAME, nosqlFile.getFileName());
			document.put(LENGTH, nosqlFile.getFileLength());
			document.put(LAST_MODIFIED, nosqlFile.getLastModified());
			document.put(BLOCK_SIZE, nosqlFile.getBlockSize());
			return document;
		}
		catch (Exception e) {
			throw new IOException("Unable to serialize file descriptor for " + nosqlFile.getFileName(), e);
		}
	}

	@Override
	public int getBlockSize() {
		return blockSize;
	}

	@Override
	public void updateFileMetadata(NosqlFile nosqlFile) throws IOException {
		MongoCollection<Document> c = getFilesCollection();

		Document query = new Document();
		query.put(FILE_NUMBER, nosqlFile.getFileNumber());

		Document object = toDocument(nosqlFile);
		c.replaceOne(query, object, new UpdateOptions().upsert(true));

	}

	@Override
	public void deleteFile(NosqlFile nosqlFile) throws IOException {
		MongoCollection<Document> c = getFilesCollection();

		Document query = new Document();
		query.put(FILE_NUMBER, nosqlFile.getFileNumber());
		c.deleteMany(query);

		MongoCollection<Document> b = getBlocksCollection();
		b.deleteMany(query);


		nameToFileMap.remove(nosqlFile.getFileName());

		nosqlFile.close();

	}

	@Override
	public void close() {
		nameToFileMap.clear();
		synchronized (MongoDirectory.class) {
			//avoid cache conflicts on segment fail back over to node which had it loaded before
			indexNameToNumberMap.remove(dbname + "-" + indexName);
		}
	}

	@Override
	public void rename(String source, String dest) throws IOException {
		MongoFile mongoFile = getFileHandle(source, false);
		mongoFile.setFileName(dest);

		updateFileMetadata(mongoFile);

		nameToFileMap.remove(source);
		nameToFileMap.put(dest, mongoFile);

	}

	@Override
	public String toString() {
		return "MongoDirectory [dbname=" + dbname + ", indexName=" + indexName + ", blockSize=" + blockSize + "]";
	}

}
