package org.lumongo.storage.rawfiles;

import org.bson.Document;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.Metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface DocumentStorage {
	void storeSourceDocument(String uniqueId, long timeStamp, Document document, List<Metadata> metaDataList) throws Exception;
	
	void storeAssociatedDocument(AssociatedDocument docs) throws Exception;
	
	List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception;
	
	AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception;

	void getAssociatedDocuments(OutputStream outputstream, Document filter) throws IOException;

	void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, long timestamp, Map<String, String> metadataMap)
					throws Exception;
	
	InputStream getAssociatedDocumentStream(String uniqueId, String filename);
	
	List<String> getAssociatedFilenames(String uniqueId) throws Exception;
	
	void deleteSourceDocument(String uniqueId) throws Exception;
	
	void deleteAssociatedDocument(String uniqueId, String fileName);
	
	void deleteAssociatedDocuments(String uniqueId);
	
	void drop();
	
	void deleteAllDocuments();

	Lumongo.ResultDocument getSourceDocument(String uniqueId, FetchType fetchType) throws Exception;
}
