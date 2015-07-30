package org.lumongo.server.indexing;

import org.lumongo.cluster.message.Lumongo;

import java.util.List;

/**
 * Created by mdavis on 7/29/15.
 */
public interface IndexDocumentStorage {
	Lumongo.ResultDocument getSourceDocument(String uniqueId, Long timestamp, Lumongo.FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask)
					throws Exception;
}
