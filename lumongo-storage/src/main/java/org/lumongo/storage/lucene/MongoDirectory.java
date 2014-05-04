package org.lumongo.storage.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.lumongo.storage.constants.MongoConstants;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

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
		DB db = mongo.getDB(dbname);
		db.getCollection(indexName + MongoDirectory.BLOCKS_SUFFIX).drop();
		db.getCollection(indexName + MongoDirectory.COUNTER_SUFFIX).drop();
		db.getCollection(indexName + MongoDirectory.FILES_SUFFIX).drop();
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
	
	public MongoDirectory(MongoClient mongo, String dbname, String indexName, boolean sharded, boolean compressed, int blockSize) throws MongoException,
					IOException {
		this.compressed = compressed;
		this.mongo = mongo;
		this.dbname = dbname;
		this.indexName = indexName;
		this.blockSize = blockSize;
		
		getFilesCollection().createIndex(new BasicDBObject(FILE_NUMBER, 1));
		getBlocksCollection().createIndex(new BasicDBObject(FILE_NUMBER, 1));
		
		DBObject indexes = new BasicDBObject();
		indexes.put(FILE_NUMBER, 1);
		indexes.put(BLOCK_NUMBER, 1);
		getBlocksCollection().createIndex(indexes);
		
		if (sharded) {
			String blockCollectionName = getBlocksCollection().getFullName();
			DB db = mongo.getDB(MongoConstants.StandardDBs.ADMIN);
			DBObject shardCommand = new BasicDBObject();
			shardCommand.put(MongoConstants.Commands.SHARD_COLLECTION, blockCollectionName);
			shardCommand.put(MongoConstants.Commands.SHARD_KEY, indexes);
			CommandResult cr = db.command(shardCommand);
			if (cr.getErrorMessage() != null) {
				System.err.println("Failed to shard <" + blockCollectionName + ">: " + cr.getErrorMessage());
			}
		}
		
		DBObject counter = new BasicDBObject();
		counter.put(_ID, FILE_COUNTER);
		
		DBCollection counterCollection = getCounterCollection();
		if (counterCollection.findOne(counter) == null) {
			counter.put(COUNTER, 0);
			counterCollection.insert(counter);
		}
		
		nameToFileMap = new ConcurrentHashMap<String, MongoFile>();
		
		fetchInitialContents();
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	private void fetchInitialContents() throws MongoException, IOException {
		DBCollection c = getFilesCollection();
		DBObject query = new BasicDBObject();
		
		DBCursor cur = c.find(query);
		while (cur.hasNext()) {
			MongoFile mf = loadFileFromDBObject(cur.next());
			nameToFileMap.put(mf.getFileName(), mf);
		}
	}
	
	public DBCollection getCounterCollection() {
		DB db = mongo.getDB(dbname);
		DBCollection c = db.getCollection(indexName + COUNTER_SUFFIX);
		return c;
	}
	
	public DBCollection getFilesCollection() {
		DB db = mongo.getDB(dbname);
		DBCollection c = db.getCollection(indexName + FILES_SUFFIX);
		return c;
	}
	
	public DBCollection getBlocksCollection() {
		DB db = mongo.getDB(dbname);
		DBCollection c = db.getCollection(indexName + BLOCKS_SUFFIX);
		return c;
	}
	
	@Override
	public String[] getFileNames() throws IOException {
		
		// System.out.println("Get file names");
		
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
		
		DBCollection c = getFilesCollection();
		
		DBObject query = new BasicDBObject();
		query.put(FILE_NAME, filename);
		DBCursor cur = c.find(query);
		
		if (cur.hasNext()) {
			return loadFileFromDBObject(cur.next());
		}
		else if (createIfNotFound) {
			return createFile(filename);
		}
		
		throw new FileNotFoundException(filename);
		
	}
	
	private int getNewFileNumber() {
		DBCollection counterCollection = getCounterCollection();
		DBObject query = new BasicDBObject();
		query.put(_ID, FILE_COUNTER);
		DBObject update = new BasicDBObject();
		DBObject increment = new BasicDBObject();
		increment.put(COUNTER, 1);
		update.put($INC, increment);
		DBObject result = counterCollection.findAndModify(query, update);
		int count = (int) result.get(COUNTER);
		return count;
	}
	
	private MongoFile createFile(String fileName) throws IOException {
		MongoFile mongoFile = new MongoFile(this, fileName, getNewFileNumber(), blockSize, compressed);
		updateFileMetadata(mongoFile);
		nameToFileMap.putIfAbsent(mongoFile.getFileName(), mongoFile);
		return nameToFileMap.get(mongoFile.getFileName());
	}
	
	private MongoFile loadFileFromDBObject(DBObject dbObject) throws IOException {
		MongoFile mongoFile = fromDbObject(dbObject);
		nameToFileMap.putIfAbsent(mongoFile.getFileName(), mongoFile);
		return nameToFileMap.get(mongoFile.getFileName());
	}
	
	public MongoFile fromDbObject(DBObject dbObject) throws IOException {
		try {
			MongoFile mongoFile = new MongoFile(this, (String) dbObject.get(FILE_NAME), (int) dbObject.get(FILE_NUMBER), (int) dbObject.get(BLOCK_SIZE),
							(boolean) dbObject.get(COMPRESSED));
			mongoFile.setFileLength((long) dbObject.get(LENGTH));
			mongoFile.setLastModified((long) dbObject.get(LAST_MODIFIED));
			return mongoFile;
		}
		catch (Exception e) {
			throw new IOException("Unable to de-serialize file descriptor from: <" + dbObject + ">: ", e);
			
		}
	}
	
	public static DBObject toDbObject(NosqlFile nosqlFile) throws IOException {
		try {
			DBObject dbObject = new BasicDBObject();
			dbObject.put(FILE_NUMBER, nosqlFile.getFileNumber());
			dbObject.put(FILE_NAME, nosqlFile.getFileName());
			dbObject.put(LENGTH, nosqlFile.getFileLength());
			dbObject.put(LAST_MODIFIED, nosqlFile.getLastModified());
			dbObject.put(BLOCK_SIZE, nosqlFile.getBlockSize());
			dbObject.put(COMPRESSED, nosqlFile.isCompressed());
			return dbObject;
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
		
		DBCollection c = getFilesCollection();
		
		DBObject query = new BasicDBObject();
		query.put(FILE_NUMBER, nosqlFile.getFileNumber());
		
		DBObject object = toDbObject(nosqlFile);
		c.update(query, object, true, false);
		
	}
	
	@Override
	public void deleteFile(NosqlFile nosqlFile) throws IOException {
		DBCollection c = getFilesCollection();
		
		DBObject query = new BasicDBObject();
		query.put(FILE_NUMBER, nosqlFile.getFileNumber());
		c.remove(query);
		
		DBCollection b = getBlocksCollection();
		b.remove(query);
		nameToFileMap.remove(nosqlFile.getFileName());
	}
	
	@Override
	public void close() {
		nameToFileMap.clear();
	}
	
	@Override
	public String toString() {
		return "MongoDirectory [dbname=" + dbname + ", indexName=" + indexName + ", blockSize=" + blockSize + "]";
	}
	
}
