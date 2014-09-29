package org.lumongo.storage.rawfiles;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BSONObject;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.storage.constants.MongoConstants;
import org.lumongo.util.CommonCompression;
import org.lumongo.util.CommonCompression.CompressionLevel;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

public class MongoDocumentStorage implements DocumentStorage {
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(MongoDocumentStorage.class);
	
	private static final String ASSOCIATED_FILES = "associatedFiles";
	private static final String FILES = "files";
	private static final String FILENAME = "filename";
	private static final String CHUNKS = "chunks";
	private static final String ASSOCIATED_METADATA = "metadata";
	
	private static final String TIMESTAMP = "_tstamp_";
	private static final String METADATA = "_meta_";
	private static final String COMPRESSED_FLAG = "_comp_";
	private static final String DOCUMENT_UNIQUE_ID_KEY = "_uid_";
	
	private MongoClient pool;
	private String database;
	private String indexName;
	
	private String rawCollectionName;
	
	public MongoDocumentStorage(MongoClient pool, String indexName, String dbName, String rawCollectionName, boolean sharded) {
		this.pool = pool;
		this.indexName = indexName;
		this.database = dbName;
		this.rawCollectionName = rawCollectionName;
		
		DB storageDb = pool.getDB(database);
		DBCollection coll = storageDb.getCollection(ASSOCIATED_FILES + "." + FILES);
		coll.createIndex(new BasicDBObject(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, 1));
		
		if (sharded) {
			
			DB adminDb = pool.getDB(MongoConstants.StandardDBs.ADMIN);
			DBObject enableCommand = new BasicDBObject();
			enableCommand.put(MongoConstants.Commands.ENABLE_SHARDING, database);
			CommandResult cr = adminDb.command(enableCommand);
			if (cr.getErrorMessage() != null) {
				System.err.println("Failed to enable sharding on the database <" + database + "> because <" + cr.getErrorMessage() + ">");
			}
			
			shardCollection(storageDb, adminDb, rawCollectionName);
			shardCollection(storageDb, adminDb, ASSOCIATED_FILES + "." + CHUNKS);
		}
	}
	
	private void shardCollection(DB db, DB adminDb, String collectionName) {
		CommandResult cr;
		DBObject shardCommand = new BasicDBObject();
		DBCollection collection = db.getCollection(collectionName);
		shardCommand.put(MongoConstants.Commands.SHARD_COLLECTION, collection.getFullName());
		shardCommand.put(MongoConstants.Commands.SHARD_KEY, new BasicDBObject(MongoConstants.StandardFields._ID, 1));
		cr = adminDb.command(shardCommand);
		if (cr.getErrorMessage() != null) {
			System.err.println("Failed to shard the collection <" + collectionName + "> because <" + cr.getErrorMessage() + ">");
		}
	}
	
	private GridFS createGridFSConnection() {
		DB db = pool.getDB(database);
		return new GridFS(db, ASSOCIATED_FILES);
	}
	
	@Override
	public void storeSourceDocument(String uniqueId, long timeStamp, BSONObject document, List<Metadata> metaDataList) throws Exception {
		DB db = pool.getDB(database);
		DBCollection coll = db.getCollection(rawCollectionName);
		DBObject object = new BasicDBObject();
		object.putAll(document);
		
		if (!metaDataList.isEmpty()) {
			DBObject metadata = new BasicDBObject();
			for (Metadata meta : metaDataList) {
				metadata.put(meta.getKey(), meta.getValue());
			}
			object.put(METADATA, metadata);
		}
		
		object.put(TIMESTAMP, timeStamp);
		object.put(MongoConstants.StandardFields._ID, uniqueId);
		
		DBObject query = new BasicDBObject(MongoConstants.StandardFields._ID, uniqueId);
		
		coll.update(query, object, true, false);
	}
	
	@Override
	public ResultDocument getSourceDocument(String uniqueId, FetchType fetchType, List<String> fieldsToReturn, List<String> fieldsToMask) throws Exception {
		if (!FetchType.NONE.equals(fetchType)) {
			DB db = pool.getDB(database);
			DBCollection coll = db.getCollection(rawCollectionName);
			DBObject search = new BasicDBObject(MongoConstants.StandardFields._ID, uniqueId);
			
			DBObject fields = null;
			
			if (FetchType.FULL.equals(fetchType)) {
				if (!fieldsToReturn.isEmpty() || !fieldsToMask.isEmpty()) {
					fields = new BasicDBObject();
					for (String fieldToReturn : fieldsToReturn) {
						fields.put(fieldToReturn, 1);
					}
					for (String fieldToMask : fieldsToMask) {
						fields.put(fieldToMask, 0);
					}
					
					fields.put(MongoConstants.StandardFields._ID, 1);
					fields.put(TIMESTAMP, 1);
					fields.put(METADATA, 1);
				}
			}
			else if (FetchType.META.equals(fetchType)) {
				fields = new BasicDBObject();
				fields.put(MongoConstants.StandardFields._ID, 1);
				fields.put(TIMESTAMP, 1);
				fields.put(METADATA, 1);
			}
			
			DBObject result = coll.findOne(search, fields);
			
			if (null != result) {
				
				long timestamp = (long) result.removeField(TIMESTAMP);
				
				ResultDocument.Builder dBuilder = ResultDocument.newBuilder();
				dBuilder.setUniqueId(uniqueId);
				dBuilder.setTimestamp(timestamp);
				
				if (result.containsField(METADATA)) {
					DBObject metadata = (DBObject) result.removeField(METADATA);
					for (String key : metadata.keySet()) {
						dBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue((String) metadata.get(key)));
					}
				}
				
				if (FetchType.FULL.equals(fetchType)) {
					ByteString document = ByteString.copyFrom(BSON.encode(result));
					dBuilder.setDocument(document);
				}
				
				dBuilder.setIndexName(indexName);
				return dBuilder.build();
			}
		}
		return null;
	}
	
	@Override
	public void deleteSourceDocument(String uniqueId) throws Exception {
		DB db = pool.getDB(database);
		DBCollection coll = db.getCollection(rawCollectionName);
		DBObject search = new BasicDBObject(MongoConstants.StandardFields._ID, uniqueId);
		coll.remove(search);
	}
	
	@Override
	public void deleteAllDocuments() {
		GridFS gridFS = createGridFSConnection();
		gridFS.remove(new BasicDBObject());
		
		DB db = pool.getDB(database);
		DBCollection coll = db.getCollection(rawCollectionName);
		coll.remove(new BasicDBObject());
	}
	
	@Override
	public void drop() {
		DB db = pool.getDB(database);
		db.dropDatabase();
	}
	
	@Override
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, long timestamp, HashMap<String, String> metadataMap)
					throws Exception {
		GridFS gridFS = createGridFSConnection();
		
		if (compress) {
			is = new DeflaterInputStream(is);
		}
		
		deleteAssociatedDocument(uniqueId, fileName);
		GridFSFile gFile = gridFS.createFile(is);
		gFile.put(MongoConstants.StandardFields._ID, getGridFsId(uniqueId, fileName));
		gFile.put(FILENAME, fileName);
		
		DBObject metadata = new BasicDBObject();
		if (metadataMap != null) {
			for (String key : metadataMap.keySet()) {
				metadata.put(key, metadataMap.get(key));
			}
		}
		metadata.put(TIMESTAMP, timestamp);
		metadata.put(COMPRESSED_FLAG, compress);
		metadata.put(DOCUMENT_UNIQUE_ID_KEY, uniqueId);
		gFile.setMetaData(metadata);
		gFile.save();
	}
	
	@Override
	public void storeAssociatedDocument(AssociatedDocument doc) throws Exception {
		GridFS gridFS = createGridFSConnection();
		
		byte[] bytes = doc.getDocument().toByteArray();
		if (doc.getCompressed()) {
			bytes = CommonCompression.compressZlib(bytes, CompressionLevel.FASTEST);
		}
		
		deleteAssociatedDocument(doc.getDocumentUniqueId(), doc.getFilename());
		GridFSFile gFile = gridFS.createFile(bytes);
		gFile.put(MongoConstants.StandardFields._ID, getGridFsId(doc.getDocumentUniqueId(), doc.getFilename()));
		gFile.put(FILENAME, doc.getFilename());
		DBObject metadata = new BasicDBObject();
		for (Metadata meta : doc.getMetadataList()) {
			metadata.put(meta.getKey(), meta.getValue());
		}
		metadata.put(TIMESTAMP, doc.getTimestamp());
		metadata.put(COMPRESSED_FLAG, doc.getCompressed());
		metadata.put(DOCUMENT_UNIQUE_ID_KEY, doc.getDocumentUniqueId());
		gFile.setMetaData(metadata);
		gFile.save();
	}
	
	@Override
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception {
		GridFS gridFS = createGridFSConnection();
		List<AssociatedDocument> assocDocs = new ArrayList<AssociatedDocument>();
		if (!FetchType.NONE.equals(fetchType)) {
			List<GridFSDBFile> files = gridFS.find(new BasicDBObject(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
			for (GridFSDBFile file : files) {
				AssociatedDocument ad = loadGridFSToAssociatedDocument(file, fetchType);
				assocDocs.add(ad);
			}
			
		}
		return assocDocs;
	}
	
	private String getGridFsId(String uniqueId, String filename) {
		return uniqueId + "-" + filename;
	}
	
	@Override
	public InputStream getAssociatedDocumentStream(String uniqueId, String fileName) {
		GridFS gridFS = createGridFSConnection();
		GridFSDBFile file = gridFS.findOne(new BasicDBObject(MongoConstants.StandardFields._ID, getGridFsId(uniqueId, fileName)));
		
		if (file == null) {
			return null;
		}
		
		InputStream is = file.getInputStream();
		
		DBObject metadata = file.getMetaData();
		if (metadata.containsField(COMPRESSED_FLAG)) {
			boolean compressed = (boolean) metadata.removeField(COMPRESSED_FLAG);
			if (compressed) {
				is = new InflaterInputStream(is);
			}
		}
		
		return is;
	}
	
	@Override
	public AssociatedDocument getAssociatedDocument(String uniqueId, String fileName, FetchType fetchType) throws Exception {
		GridFS gridFS = createGridFSConnection();
		if (!FetchType.NONE.equals(fetchType)) {
			GridFSDBFile file = gridFS.findOne(new BasicDBObject(MongoConstants.StandardFields._ID, getGridFsId(uniqueId, fileName)));
			if (null != file) {
				return loadGridFSToAssociatedDocument(file, fetchType);
			}
		}
		return null;
	}
	
	private AssociatedDocument loadGridFSToAssociatedDocument(GridFSDBFile file, FetchType fetchType) throws IOException {
		AssociatedDocument.Builder aBuilder = AssociatedDocument.newBuilder();
		aBuilder.setFilename(file.getFilename());
		DBObject metadata = file.getMetaData();
		
		boolean compressed = false;
		if (metadata.containsField(COMPRESSED_FLAG)) {
			compressed = (boolean) metadata.removeField(COMPRESSED_FLAG);
		}
		
		long timestamp = (long) metadata.removeField(TIMESTAMP);
		
		aBuilder.setCompressed(compressed);
		aBuilder.setTimestamp(timestamp);
		
		aBuilder.setDocumentUniqueId((String) metadata.removeField(DOCUMENT_UNIQUE_ID_KEY));
		for (String field : metadata.keySet()) {
			aBuilder.addMetadata(Metadata.newBuilder().setKey(field).setValue((String) metadata.get(field)));
		}
		
		if (FetchType.FULL.equals(fetchType)) {
			byte[] bytes = MongoConstants.Functions.readFileFromGridFS(file);
			if (null != bytes) {
				if (compressed) {
					bytes = CommonCompression.uncompressZlib(bytes);
				}
				aBuilder.setDocument(ByteString.copyFrom(bytes));
			}
		}
		aBuilder.setIndexName(indexName);
		return aBuilder.build();
	}
	
	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		GridFS gridFS = createGridFSConnection();
		ArrayList<String> fileNames = new ArrayList<String>();
		List<GridFSDBFile> files = gridFS.find(new BasicDBObject(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
		for (GridFSDBFile file : files) {
			fileNames.add(file.getFilename());
		}
		return fileNames;
	}
	
	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) {
		GridFS gridFS = createGridFSConnection();
		gridFS.remove(new BasicDBObject(MongoConstants.StandardFields._ID, getGridFsId(uniqueId, fileName)));
		
	}
	
	@Override
	public void deleteAssociatedDocuments(String uniqueId) {
		GridFS gridFS = createGridFSConnection();
		gridFS.remove(new BasicDBObject(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
	}
	
}
