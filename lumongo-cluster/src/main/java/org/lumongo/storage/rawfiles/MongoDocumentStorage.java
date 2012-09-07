package org.lumongo.storage.rawfiles;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

import org.bson.BSON;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;
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
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

public class MongoDocumentStorage implements DocumentStorage {
	public static final String STORAGE_DB_SUFFIX = "_storage";
	
	private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");
	private static final String ASSOCIATED_FILES = "associatedFiles";
	private static final String FILES = "files";
	private static final String FILENAME = "filename";
	private static final String VALUE = "value";
	private static final String DOC = "doc";
	private static final String TYPE = "type";
	private static final String COMPRESSED = "compressed";
	private static final String METADATA = "metadata";
	private static final String RESULT_STORAGE_COLLECTION = "resultStorage";
	private static final String UNIQUE_ID_KEY = "unique_id";
	private static final String CHUNKS = "chunks";
	
	private Mongo pool;
	private String database;
	
	// TODO: make this configurable?
	private WriteConcern documentWriteConcern = WriteConcern.NORMAL;
	
	public MongoDocumentStorage(Mongo pool, String database) {
		this(pool, database, false);
	}
	
	public MongoDocumentStorage(Mongo pool, String database, boolean sharded) {
		this.pool = pool;
		this.database = database;
		
		if (sharded) {
			DB storageDb = pool.getDB(database);
			
			DBCollection coll = storageDb.getCollection(ASSOCIATED_FILES + "." + FILES);
			coll.ensureIndex(METADATA + "." + UNIQUE_ID_KEY);
			
			DB adminDb = pool.getDB(MongoConstants.StandardDBs.ADMIN);
			DBObject enableCommand = new BasicDBObject();
			enableCommand.put(MongoConstants.Commands.ENABLE_SHARDING, database);
			CommandResult cr = adminDb.command(enableCommand);
			if (cr.getErrorMessage() != null) {
				System.err.println("Failed to enable sharding on the database <" + database + "> because <" + cr.getErrorMessage() + ">");
			}
			
			shardCollection(storageDb, adminDb, RESULT_STORAGE_COLLECTION);
			shardCollection(storageDb, adminDb, ASSOCIATED_FILES + "." + CHUNKS);
		}
	}
	
	public MongoDocumentStorage(String mongoHost, int mongoPort, String database, boolean sharded) throws UnknownHostException, MongoException {
		this(new Mongo(mongoHost, mongoPort), database, sharded);
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
	public void storeSourceDocument(String uniqueId, ResultDocument doc) throws Exception {
		DB db = pool.getDB(database);
		DBCollection coll = db.getCollection(RESULT_STORAGE_COLLECTION);
		DBObject document = new BasicDBObject();
		if (!doc.getCompressed() && doc.getType().equals(ResultDocument.Type.BSON)) {
			document.putAll(BSON.decode(doc.getDocument().toByteArray()));
		}
		else if (!doc.getCompressed() && doc.getType().equals(ResultDocument.Type.TEXT)) {
			document.put(DOC, new String(doc.getDocument().toByteArray(), UTF_8_CHARSET));
		}
		else {
			byte[] bytes = doc.getDocument().toByteArray();
			if (doc.getCompressed()) {
				bytes = CommonCompression.compressZlib(bytes, CompressionLevel.NORMAL);
			}
			document.put(DOC, bytes);
		}
		
		if (doc.getMetadataCount() > 0) {
			DBObject metadata = new BasicDBObject();
			for (Metadata meta : doc.getMetadataList()) {
				metadata.put(meta.getKey(), meta.getValue());
			}
			document.put(METADATA, metadata);
		}
		
		document.put(COMPRESSED, doc.getCompressed());
		document.put(TYPE, doc.getType().toString());
		document.put(MongoConstants.StandardFields._ID, uniqueId);
		coll.save(document, documentWriteConcern);
	}
	
	@Override
	public ResultDocument getSourceDocument(String uniqueId, FetchType fetchType) throws Exception {
		if (!FetchType.NONE.equals(fetchType)) {
			DB db = pool.getDB(database);
			DBCollection coll = db.getCollection(RESULT_STORAGE_COLLECTION);
			DBObject search = new BasicDBObject(MongoConstants.StandardFields._ID, uniqueId);
			DBObject result = coll.findOne(search);
			if (null != result) {
				boolean compressed = (boolean) result.removeField(COMPRESSED);
				
				ResultDocument.Type type = ResultDocument.Type.valueOf((String) result.removeField(TYPE));
				ResultDocument.Builder dBuilder = ResultDocument.newBuilder();
				dBuilder.setType(type);
				dBuilder.setCompressed(compressed);
				dBuilder.setUniqueId(uniqueId);
				
				if (result.containsField(METADATA)) {
					DBObject metadata = (DBObject) result.removeField(METADATA);
					for (String key : metadata.keySet()) {
						dBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue((String) metadata.get(VALUE)).build());
					}
				}
				if (FetchType.FULL.equals(fetchType)) {
					ByteString document = null;
					if (!compressed && type.equals(ResultDocument.Type.BSON)) {
						document = ByteString.copyFrom(BSON.encode(result));
					}
					else if (!compressed && type.equals(ResultDocument.Type.TEXT)) {
						document = ByteString.copyFrom(((String) result.get(DOC)).getBytes(UTF_8_CHARSET));
					}
					else {
						byte[] bytes = (byte[]) result.get(DOC);
						if (compressed) {
							bytes = CommonCompression.uncompressZlib(bytes);
						}
						document = ByteString.copyFrom(bytes);
					}
					dBuilder.setDocument(document);
				}
				
				return dBuilder.build();
			}
		}
		return null;
	}
	
	@Override
	public void deleteSourceDocument(String uniqueId) throws Exception {
		DB db = pool.getDB(database);
		DBCollection coll = db.getCollection(RESULT_STORAGE_COLLECTION);
		DBObject search = new BasicDBObject(MongoConstants.StandardFields._ID, uniqueId);
		coll.remove(search, documentWriteConcern);
	}
	
	@Override
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, HashMap<String, String> metadataMap)
			throws Exception {
		GridFS gridFS = createGridFSConnection();
		
		if (compress) {
			is = new DeflaterInputStream(is);
		}
		
		GridFSFile gFile = gridFS.createFile(is);
		gFile.put(MongoConstants.StandardFields._ID, getGridFsId(uniqueId, fileName));
		gFile.put(FILENAME, fileName);
		
		DBObject metadata = new BasicDBObject();
		if (metadataMap != null) {
			for (String key : metadataMap.keySet()) {
				metadata.put(key, metadataMap.get(key));
			}
		}
		metadata.put(COMPRESSED, compress);
		metadata.put(UNIQUE_ID_KEY, uniqueId);
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
		
		GridFSFile gFile = gridFS.createFile(bytes);
		gFile.put(MongoConstants.StandardFields._ID, getGridFsId(doc.getDocumentUniqueId(), doc.getFilename()));
		gFile.put(FILENAME, doc.getFilename());
		DBObject metadata = new BasicDBObject();
		for (Metadata meta : doc.getMetadataList()) {
			metadata.put(meta.getKey(), meta.getValue());
		}
		metadata.put(COMPRESSED, doc.getCompressed());
		metadata.put(UNIQUE_ID_KEY, doc.getDocumentUniqueId());
		gFile.setMetaData(metadata);
		gFile.save();
	}
	
	@Override
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception {
		GridFS gridFS = createGridFSConnection();
		List<AssociatedDocument> assocDocs = new ArrayList<AssociatedDocument>();
		if (!FetchType.NONE.equals(fetchType)) {
			List<GridFSDBFile> files = gridFS.find(new BasicDBObject(METADATA + "." + UNIQUE_ID_KEY, uniqueId));
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
		if (metadata.containsField(COMPRESSED)) {
			boolean compressed = (boolean) metadata.removeField(COMPRESSED);
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
		if (metadata.containsField(COMPRESSED)) {
			compressed = (boolean) metadata.removeField(COMPRESSED);
		}
		aBuilder.setCompressed(compressed);
		
		aBuilder.setDocumentUniqueId((String) metadata.get(UNIQUE_ID_KEY));
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
		return aBuilder.build();
	}
	
	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		GridFS gridFS = createGridFSConnection();
		ArrayList<String> fileNames = new ArrayList<String>();
		List<GridFSDBFile> files = gridFS.find(new BasicDBObject(METADATA + "." + UNIQUE_ID_KEY, uniqueId));
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
		gridFS.remove(new BasicDBObject(METADATA + "." + UNIQUE_ID_KEY, uniqueId));
	}
}
