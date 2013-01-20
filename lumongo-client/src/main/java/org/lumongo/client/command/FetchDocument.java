package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;

public class FetchDocument extends Fetch {

    public FetchDocument(String uniqueId) {
        super(uniqueId);
        setResultFetchType(FetchType.FULL);
        setAssociatedFetchType(FetchType.NONE);
    }

}
