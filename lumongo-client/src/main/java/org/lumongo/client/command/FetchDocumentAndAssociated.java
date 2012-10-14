package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;

public class FetchDocumentAndAssociated extends Fetch {

    public FetchDocumentAndAssociated(String uniqueId) {
        this(uniqueId, false);
    }

    public FetchDocumentAndAssociated(String uniqueId, boolean metaOnly) {
        super(uniqueId);
        setResultFetchType(FetchType.FULL);
        if (metaOnly) {
            setAssociatedFetchType(FetchType.META);
        }
        else {
            setAssociatedFetchType(FetchType.FULL);
        }
    }

}
