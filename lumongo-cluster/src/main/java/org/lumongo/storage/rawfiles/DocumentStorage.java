package org.lumongo.storage.rawfiles;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;
import org.lumongo.cluster.message.Lumongo.ResultDocument;

public interface DocumentStorage {
	public void storeSourceDocument(String uniqueId, ResultDocument doc) throws Exception;
	
	public void storeAssociatedDocument(AssociatedDocument docs) throws Exception;
	
	public ResultDocument getSourceDocument(String uniqueId, FetchType fetchType) throws Exception;
	
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception;
	
	public AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception;
	
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, HashMap<String, String> metadataMap)
			throws Exception;
	
	public InputStream getAssociatedDocumentStream(String uniqueId, String filename);
	
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception;
	
	public void deleteSourceDocument(String uniqueId) throws Exception;
	
	public void deleteAssociatedDocument(String uniqueId, String fileName);
	
	public void deleteAssociatedDocuments(String uniqueId);
	
}
