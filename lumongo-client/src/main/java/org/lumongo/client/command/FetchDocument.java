package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

public class FetchDocument extends Fetch {
	
	public FetchDocument(ScoredResult sr) {
		this(sr.getUniqueId(), sr.getIndexName());
	}
	
	public FetchDocument(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setResultFetchType(FetchType.FULL);
		setAssociatedFetchType(FetchType.NONE);
	}
	
}
