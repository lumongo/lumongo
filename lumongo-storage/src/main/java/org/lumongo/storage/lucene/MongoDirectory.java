package org.lumongo.storage.lucene;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

	private static final String $INC = "$inc";
	private static final String _ID = "_id";
	private static final String FILE_COUNTER = "fileCounter";
	private static final String COUNTER = "counter";

	public static final String BLOCK_SIZE = "blockSize";
	public static final String BLOCK_NUMBER = "blockNumber";
	public static final String LAST_MODIFIED = "lastModified";
	public static final String LENGTH = "length";
	public static final String FILE_NAME = "fileName";
	public static final String FILE_NUMBER = "fileNumber";
	public static final String COMPRESSED = "compressed";

	public static String BYTES = "bytes";

	public static final String FILES_SUFFIX = ".files";
	public static final String BLOCKS_SUFFIX = ".blocks";
	public static final String COUNTER_SUFFIX = ".counter";

	private final MongoClient mongo;
	private final String dbname;
	private final String indexName;
	private final int blockSize;
	private final boolean compressed;

	public static final int DEFAULT_BLOCK_SIZE = 1024 * 128;
	public static final int DEFAULT_BLOCK_MAX = 12500;

	private ConcurrentHashMap<String, MongoFile> nameToFileMap;
	private static Cache<MongoBlock, MongoBlock> blockCache;

	/**
	 * Removes an index from a database
	 * @param mongo
	 * @param dbname
	 * @param indexName
	 */
	public static void dropIndex(MongoClient mongo, String dbname, String indexName) {
		MongoDatabase db = mongo.getDatabase(dbname);
		db.getCollection(indexName + MongoDirectory.BLOCKS_SUFFIX).dropCollection();
		db.getCollection(indexName + MongoDirectory.COUNTER_SUFFIX).dropCollection();
		db.getCollection(indexName + MongoDirectory.FILES_SUFFIX).dropCollection();
	}

	public static void setMaxIndexBlocks(int blocks) {
		ConcurrentMap<MongoBlock, MongoBlock> oldMap = null;
		if (blockCache != null) {
			oldMap = blockCache.asMap();
		}
		RemovalListener<MongoBlock, MongoBlock> listener = new RemovalListener<MongoBlock, MongoBlock>() {

			@Override
			public void onRemoval(RemovalNotification<MongoBlock, MongoBlock> notification) {
				if (RemovalCause.SIZE.equals(notification.getCause())) {
					MongoBlock mb = notification.getKey();

					if (mb.dirty) {
						mb.storeBlock();
					}
				}
			}
		};
		blockCache = CacheBuilder.newBuilder().concurrencyLevel(16).maximumSize(blocks).removalListener(listener).build();
		if (oldMap != null) {
			blockCache.asMap().putAll(oldMap);
			oldMap.clear();
		}
	}

	static {
		setMaxIndexBlocks(DEFAULT_BLOCK_MAX);
	}

	public static void cacheBlock(MongoBlock mb) {
		blockCache.put(mb, mb);
	}

	public static void removeBlock(MongoBlock mb) {
		blockCache.invalidate(mb);
	}

	public MongoDirectory(MongoClient mongo, String dbname, String indexName) throws MongoException, IOException {
		this(mongo, dbname, indexName, false, false);
	}

	public MongoDirectory(MongoClient mongo, String dbname, String indexName, boolean sharded, boolean compressed) throws MongoException, IOException {
		this(mongo, dbname, indexName, sharded, compressed, DEFAULT_BLOCK_SIZE);
	}

	public MongoDirectory(MongoClient mongo, String dbname, String indexName, boolean sharded, boolean compressed, int blockSize)
					throws MongoException, IOException {
		this.compressed = compressed;
		this.mongo = mongo;
		this.dbname = dbname;
		this.indexName = indexName;
		this.blockSize = blockSize;

		getFilesCollection().createIndex(new Document(FILE_NUMBER, 1));
		getBlocksCollection().createIndex(new Document(FILE_NUMBER, 1));

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
			db.executeCommand(shardCommand);
		}

		Document counter = new Document();
		counter.put(_ID, FILE_COUNTER);

		MongoCollection<Document> counterCollection = getCounterCollection();
		if (counterCollection.find(counter).first() == null) {
			counter.put(COUNTER, 0);
			counterCollection.insertOne(counter);
		}

		nameToFileMap = new ConcurrentHashMap<String, MongoFile>();

		fetchInitialContents();
	}

	public String getIndexName() {
		return indexName;
	}

	private void fetchInitialContents() throws MongoException, IOException {
		MongoCollection<Document> c = getFilesCollection();
		Document query = new Document();

		FindIterable<Document> cur = c.find(query);
		for (Document d : cur) {
			MongoFile mf = loadFileFromDBObject(d);
			nameToFileMap.put(mf.getFileName(), mf);
		}
	}

	public MongoCollection<Document> getCounterCollection() {
		MongoDatabase db = mongo.getDatabase(dbname);
		MongoCollection<Document> c = db.getCollection(indexName + COUNTER_SUFFIX);
		return c;
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

		return nameToFileMap.keySet().toArray(new String[0]);
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

	private int getNewFileNumber() {
		MongoCollection<Document> counterCollection = getCounterCollection();

		Document query = new Document();
		query.put(_ID, FILE_COUNTER);

		Document update = new Document();
		Document increment = new Document();
		increment.put(COUNTER, 1);
		update.put($INC, increment);

		Document result = counterCollection.findOneAndUpdate(query, update);
		int count = (int) result.get(COUNTER);

		return count;
	}

	private MongoFile createFile(String fileName) throws IOException {
		MongoFile mongoFile = new MongoFile(this, fileName, getNewFileNumber(), blockSize, compressed);
		updateFileMetadata(mongoFile);
		nameToFileMap.putIfAbsent(mongoFile.getFileName(), mongoFile);
		return nameToFileMap.get(mongoFile.getFileName());
	}

	private MongoFile loadFileFromDBObject(Document document) throws IOException {
		MongoFile mongoFile = fromDocument(document);
		nameToFileMap.putIfAbsent(mongoFile.getFileName(), mongoFile);
		return nameToFileMap.get(mongoFile.getFileName());
	}

	public MongoFile fromDocument(Document document) throws IOException {
		try {
			MongoFile mongoFile = new MongoFile(this, (String) document.get(FILE_NAME), (int) document.get(FILE_NUMBER), (int) document.get(BLOCK_SIZE),
							(boolean) document.get(COMPRESSED));
			mongoFile.setFileLength((long) document.get(LENGTH));
			mongoFile.setLastModified((long) document.get(LAST_MODIFIED));
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
			document.put(COMPRESSED, nosqlFile.isCompressed());
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
	}

	@Override
	public void close() {
		nameToFileMap.clear();
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
