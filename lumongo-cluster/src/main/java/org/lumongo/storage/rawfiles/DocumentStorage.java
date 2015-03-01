package org.lumongo.storage.rawfiles;

import org.bson.BasicBSONObject;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

public interface DocumentStorage {
	public void storeSourceDocument(String uniqueId, long timeStamp, BasicBSONObject document, List<Metadata> metaDataList) throws Exception;
	
	public void storeAssociatedDocument(AssociatedDocument docs) throws Exception;
	
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception;
	
	public AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception;
	
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, long timestamp, HashMap<String, String> metadataMap)
					throws Exception;
	
	public InputStream getAssociatedDocumentStream(String uniqueId, String filename);
	
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception;
	
	public void deleteSourceDocument(String uniqueId) throws Exception;
	
	public void deleteAssociatedDocument(String uniqueId, String fileName);
	
	public void deleteAssociatedDocuments(String uniqueId);
	
	public void drop();
	
	public void deleteAllDocuments();
	
	public ResultDocument getSourceDocument(String uniqueId, FetchType fetchType, List<String> fieldsToReturn, List<String> fieldsToMask) throws Exception;
}
