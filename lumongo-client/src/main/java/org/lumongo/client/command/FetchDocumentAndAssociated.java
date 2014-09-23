package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchType;

public class FetchDocumentAndAssociated extends Fetch {
	
	public FetchDocumentAndAssociated(String uniqueId, String indexName) {
		this(uniqueId, indexName, false);
	}
	
	public FetchDocumentAndAssociated(String uniqueId, String indexName, boolean metaOnly) {
		super(uniqueId, indexName);
		setResultFetchType(FetchType.FULL);
		if (metaOnly) {
			setAssociatedFetchType(FetchType.META);
		}
		else {
			setAssociatedFetchType(FetchType.FULL);
		}
	}
	
}
