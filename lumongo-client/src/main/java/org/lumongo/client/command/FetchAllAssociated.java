package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;

public class FetchAllAssociated extends Fetch {

    public FetchAllAssociated(String uniqueId) {
        super(uniqueId);        
        setResultFetchType(FetchType.NONE);
        setAssociatedFetchType(FetchType.FULL);
    }

}
