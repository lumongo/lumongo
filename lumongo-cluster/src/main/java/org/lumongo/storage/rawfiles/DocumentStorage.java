package org.lumongo.storage.rawfiles;

import org.bson.BasicBSONObject;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.server.indexing.ResultBundle;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

public interface DocumentStorage {
	void storeSourceDocument(String uniqueId, long timeStamp, BasicBSONObject document, List<Metadata> metaDataList) throws Exception;
	
	void storeAssociatedDocument(AssociatedDocument docs) throws Exception;
	
	List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception;
	
	AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception;
	
	void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, long timestamp, HashMap<String, String> metadataMap)
					throws Exception;
	
	InputStream getAssociatedDocumentStream(String uniqueId, String filename);
	
	List<String> getAssociatedFilenames(String uniqueId) throws Exception;
	
	void deleteSourceDocument(String uniqueId) throws Exception;
	
	void deleteAssociatedDocument(String uniqueId, String fileName);
	
	void deleteAssociatedDocuments(String uniqueId);
	
	void drop();
	
	void deleteAllDocuments();
	
	ResultBundle getSourceDocument(String uniqueId, FetchType fetchType) throws Exception;
}
