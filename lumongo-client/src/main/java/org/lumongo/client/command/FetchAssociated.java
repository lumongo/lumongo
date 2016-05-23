package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchType;

public class FetchAssociated extends Fetch {
	
	public FetchAssociated(String uniqueId, String indexName, String fileName) {
		super(uniqueId, indexName);
		setFilename(fileName);
		setResultFetchType(FetchType.NONE);
		setAssociatedFetchType(FetchType.FULL);
	}
	
}
