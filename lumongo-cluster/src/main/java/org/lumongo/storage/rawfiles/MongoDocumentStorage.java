package org.lumongo.storage.rawfiles;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.UpdateOptions;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.Document;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.storage.constants.MongoConstants;
import org.lumongo.util.CommonCompression;
import org.lumongo.util.CommonCompression.CompressionLevel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

public class MongoDocumentStorage implements DocumentStorage {
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(MongoDocumentStorage.class);

	private static final String ASSOCIATED_FILES = "associatedFiles";
	private static final String FILES = "files";
	private static final String CHUNKS = "chunks";
	private static final String ASSOCIATED_METADATA = "metadata";

	private static final String TIMESTAMP = "_tstamp_";
	private static final String METADATA = "_meta_";
	private static final String COMPRESSED_FLAG = "_comp_";
	private static final String DOCUMENT_UNIQUE_ID_KEY = "_uid_";
	private static final String FILE_UNIQUE_ID_KEY = "_fid_";

	private MongoClient mongoClient;
	private String database;
	private String indexName;

	private String rawCollectionName;

	public MongoDocumentStorage(MongoClient mongoClient, String indexName, String dbName, String rawCollectionName, boolean sharded) {
		this.mongoClient = mongoClient;
		this.indexName = indexName;
		this.database = dbName;
		this.rawCollectionName = rawCollectionName;

		MongoDatabase storageDb = mongoClient.getDatabase(database);
		MongoCollection<Document> coll = storageDb.getCollection(ASSOCIATED_FILES + "." + FILES);
		coll.createIndex(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, 1));

		if (sharded) {

			MongoDatabase adminDb = mongoClient.getDatabase(MongoConstants.StandardDBs.ADMIN);
			Document enableCommand = new Document();
			enableCommand.put(MongoConstants.Commands.ENABLE_SHARDING, database);
			adminDb.runCommand(enableCommand);

			shardCollection(storageDb, adminDb, rawCollectionName);
			shardCollection(storageDb, adminDb, ASSOCIATED_FILES + "." + CHUNKS);
		}
	}

	private void shardCollection(MongoDatabase db, MongoDatabase adminDb, String collectionName) {
		Document shardCommand = new Document();
		MongoCollection<Document> collection = db.getCollection(collectionName);
		shardCommand.put(MongoConstants.Commands.SHARD_COLLECTION, collection.getNamespace().getFullName());
		shardCommand.put(MongoConstants.Commands.SHARD_KEY, new BasicDBObject(MongoConstants.StandardFields._ID, 1));
		adminDb.runCommand(shardCommand);
	}

	private GridFSBucket createGridFSConnection() {
		MongoDatabase db = mongoClient.getDatabase(database);
		return GridFSBuckets.create(db, ASSOCIATED_FILES);
	}

	@Override
	public void storeSourceDocument(String uniqueId, long timeStamp, Document document, List<Metadata> metaDataList) throws Exception {
		MongoDatabase db = mongoClient.getDatabase(database);
		MongoCollection<Document> coll = db.getCollection(rawCollectionName);
		Document mongoDocument = new Document();
		mongoDocument.putAll(document);

		if (!metaDataList.isEmpty()) {
			Document metadataMongoDoc = new Document();
			for (Metadata meta : metaDataList) {
				metadataMongoDoc.put(meta.getKey(), meta.getValue());
			}
			mongoDocument.put(METADATA, metadataMongoDoc);
		}

		mongoDocument.put(TIMESTAMP, timeStamp);
		mongoDocument.put(MongoConstants.StandardFields._ID, uniqueId);

		Document query = new Document(MongoConstants.StandardFields._ID, uniqueId);

		coll.replaceOne(query, mongoDocument, new UpdateOptions().upsert(true));
	}

	@Override
	public Lumongo.ResultDocument getSourceDocument(String uniqueId, FetchType fetchType) throws Exception {
		if (!FetchType.NONE.equals(fetchType)) {
			MongoDatabase db = mongoClient.getDatabase(database);
			MongoCollection<Document> coll = db.getCollection(rawCollectionName);
			Document search = new Document(MongoConstants.StandardFields._ID, uniqueId);

			Document result = coll.find(search).first();

			if (null != result) {

				long timestamp = (long) result.remove(TIMESTAMP);

				ResultDocument.Builder dBuilder = ResultDocument.newBuilder();
				dBuilder.setUniqueId(uniqueId);
				dBuilder.setTimestamp(timestamp);

				if (result.containsKey(METADATA)) {
					Document metadata = (Document) result.remove(METADATA);
					for (String key : metadata.keySet()) {
						dBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue((String) metadata.get(key)));
					}
				}

				if (FetchType.FULL.equals(fetchType)) {
					BasicDBObject resultObj = new BasicDBObject();
					resultObj.putAll(result);

					ByteString document = ByteString.copyFrom(BSON.encode(resultObj));
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
		MongoDatabase db = mongoClient.getDatabase(database);
		MongoCollection<Document> coll = db.getCollection(rawCollectionName);
		Document search = new Document(MongoConstants.StandardFields._ID, uniqueId);
		coll.deleteOne(search);
	}

	@Override
	public void deleteAllDocuments() {
		GridFSBucket gridFS = createGridFSConnection();
		gridFS.drop();

		MongoDatabase db = mongoClient.getDatabase(database);
		MongoCollection<Document> coll = db.getCollection(rawCollectionName);
		coll.deleteMany(new Document());
	}

	@Override
	public void drop() {
		MongoDatabase db = mongoClient.getDatabase(database);
		db.drop();
	}

	@Override
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, long timestamp, Map<String, String> metadataMap)
			throws Exception {
		GridFSBucket gridFS = createGridFSConnection();

		if (compress) {
			is = new DeflaterInputStream(is);
		}

		deleteAssociatedDocument(uniqueId, fileName);

		GridFSUploadOptions gridFSUploadOptions = getGridFSUploadOptions(uniqueId, fileName, compress, timestamp, metadataMap);
		gridFS.uploadFromStream(fileName, is, gridFSUploadOptions);
	}

	private GridFSUploadOptions getGridFSUploadOptions(String uniqueId, String fileName, boolean compress, long timestamp,
			Map<String, String> metadataMap) {
		Document metadata = new Document();
		if (metadataMap != null) {
			for (String key : metadataMap.keySet()) {
				metadata.put(key, metadataMap.get(key));
			}
		}
		metadata.put(TIMESTAMP, timestamp);
		metadata.put(COMPRESSED_FLAG, compress);
		metadata.put(DOCUMENT_UNIQUE_ID_KEY, uniqueId);
		metadata.put(FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName));

		return new GridFSUploadOptions().chunkSizeBytes(1024).metadata(metadata);
	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument doc) throws Exception {

		byte[] bytes = doc.getDocument().toByteArray();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);

		Map<String, String> metadata = new HashMap<>();
		for (Metadata meta : doc.getMetadataList()) {
			metadata.put(meta.getKey(), meta.getValue());
		}

		storeAssociatedDocument(doc.getDocumentUniqueId(), doc.getFilename(), byteInputStream, doc.getCompressed(), doc.getTimestamp(), metadata);
	}

	@Override
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception {
		GridFSBucket gridFS = createGridFSConnection();
		List<AssociatedDocument> assocDocs = new ArrayList<>();
		if (!FetchType.NONE.equals(fetchType)) {
			GridFSFindIterable files = gridFS.find(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
			for (GridFSFile file : files) {
				AssociatedDocument ad = loadGridFSToAssociatedDocument(gridFS, file, fetchType);
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
		GridFSBucket gridFS = createGridFSConnection();
		GridFSFile file = gridFS.find(new Document(ASSOCIATED_METADATA + "." + FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName))).first();

		if (file == null) {
			return null;
		}

		InputStream is = gridFS.openDownloadStream(file.getObjectId());;

		Document metadata = file.getMetadata();
		if (metadata.containsKey(COMPRESSED_FLAG)) {
			boolean compressed = (boolean) metadata.remove(COMPRESSED_FLAG);
			if (compressed) {
				is = new InflaterInputStream(is);
			}
		}

		return is;
	}

	@Override
	public AssociatedDocument getAssociatedDocument(String uniqueId, String fileName, FetchType fetchType) throws Exception {
		GridFSBucket gridFS = createGridFSConnection();
		if (!FetchType.NONE.equals(fetchType)) {
			GridFSFile file = gridFS
					.find(new Document(ASSOCIATED_METADATA + "." + FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName))).first();
			if (null != file) {
				return loadGridFSToAssociatedDocument(gridFS, file, fetchType);
			}
		}
		return null;
	}

	private AssociatedDocument loadGridFSToAssociatedDocument(GridFSBucket gridFS, GridFSFile file, FetchType fetchType) throws IOException {
		AssociatedDocument.Builder aBuilder = AssociatedDocument.newBuilder();
		aBuilder.setFilename(file.getFilename());
		Document metadata = file.getMetadata();

		boolean compressed = false;
		if (metadata.containsKey(COMPRESSED_FLAG)) {
			compressed = (boolean) metadata.remove(COMPRESSED_FLAG);
		}

		long timestamp = (long) metadata.remove(TIMESTAMP);

		aBuilder.setCompressed(compressed);
		aBuilder.setTimestamp(timestamp);

		aBuilder.setDocumentUniqueId((String) metadata.remove(DOCUMENT_UNIQUE_ID_KEY));
		for (String field : metadata.keySet()) {
			aBuilder.addMetadata(Metadata.newBuilder().setKey(field).setValue((String) metadata.get(field)));
		}

		if (FetchType.FULL.equals(fetchType)) {

			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			gridFS.downloadToStream(file.getObjectId(), byteArrayOutputStream);
			byte[] bytes = byteArrayOutputStream.toByteArray();
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


	public void getAssociatedDocuments(OutputStream outputstream) throws Exception {
		Charset charset = Charset.forName("UTF-8");

		GridFSBucket gridFS = createGridFSConnection();
		GridFSFindIterable gridFSFiles = gridFS.find();
		outputstream.write('[');

		boolean first = true;
		for (GridFSFile gridFSFile : gridFSFiles) {
			if (first) {
				first = false;
			}
			else {
				outputstream.write(',');
			}

			String uniqueId = gridFSFile.getMetadata().getString(DOCUMENT_UNIQUE_ID_KEY);
			String uniquieIdKeyValue = "{ uniqueId: " + uniqueId + ", ";
			outputstream.write(uniquieIdKeyValue.getBytes(charset));

			String filename = gridFSFile.getFilename();
			String filenameKeyValue = "filename: " + filename + " }";
			outputstream.write(filenameKeyValue.getBytes(charset));

		}
		outputstream.write(']');
	}

	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		GridFSBucket gridFS = createGridFSConnection();
		ArrayList<String> fileNames = new ArrayList<>();
		gridFS.find(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId))
				.forEach((Consumer<com.mongodb.client.gridfs.model.GridFSFile>) gridFSFile -> fileNames.add(gridFSFile.getFilename()));

		return fileNames;
	}

	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) {
		GridFSBucket gridFS = createGridFSConnection();
		gridFS.find(new Document(ASSOCIATED_METADATA + "." + FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName)))
				.forEach((Block<com.mongodb.client.gridfs.model.GridFSFile>) gridFSFile -> gridFS.delete(gridFSFile.getObjectId()));

	}

	@Override
	public void deleteAssociatedDocuments(String uniqueId) {
		GridFSBucket gridFS = createGridFSConnection();
		gridFS.find(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId))
				.forEach((Block<com.mongodb.client.gridfs.model.GridFSFile>) gridFSFile -> gridFS.delete(gridFSFile.getObjectId()));
	}

}
