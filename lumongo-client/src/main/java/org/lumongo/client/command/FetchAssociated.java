package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;

public class FetchAssociated extends Fetch {

    public FetchAssociated(String uniqueId, String fileName) {
        super(uniqueId);
        setFileName(fileName);
        setResultFetchType(FetchType.NONE);
        setAssociatedFetchType(FetchType.FULL);
    }

}
