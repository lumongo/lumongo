package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

public class FetchDocument extends Fetch {

	public FetchDocument(ScoredResult sr) {
		this(sr.getUniqueId());
	}

    public FetchDocument(String uniqueId) {
        super(uniqueId);
        setResultFetchType(FetchType.FULL);
        setAssociatedFetchType(FetchType.NONE);
    }

}
