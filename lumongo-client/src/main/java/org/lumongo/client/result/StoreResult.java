package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.StoreResponse;

public class StoreResult extends Result {

    // Nothing in response currently
    @SuppressWarnings("unused")
    private StoreResponse storeResponse;

    public StoreResult(StoreResponse storeResponse, long commandTimeMs) {
        super(commandTimeMs);
        this.storeResponse = storeResponse;
    }

}
