package org.lumongo.client.result;

import java.util.List;

import org.lumongo.cluster.message.Lumongo.GetIndexesResponse;

public class GetIndexesResult extends Result {

    private GetIndexesResponse getIndexesResponse;

    public GetIndexesResult(GetIndexesResponse getIndexesResponse) {
        this.getIndexesResponse = getIndexesResponse;
    }

    public List<String> getIndexNames() {
        return getIndexesResponse.getIndexNameList();
    }

    public boolean containsIndex(String indexName) {
        return getIndexesResponse.getIndexNameList().contains(indexName);
    }

    public int getIndexCount() {
        return getIndexesResponse.getIndexNameCount();
    }
}
