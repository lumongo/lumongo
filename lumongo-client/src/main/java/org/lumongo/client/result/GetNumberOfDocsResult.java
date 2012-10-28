package org.lumongo.client.result;

import java.util.List;

import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;

public class GetNumberOfDocsResult extends Result {

    private GetNumberOfDocsResponse getNumberOfDocsResponse;

    public GetNumberOfDocsResult(GetNumberOfDocsResponse getNumberOfDocsResponse) {
        this.getNumberOfDocsResponse = getNumberOfDocsResponse;
    }

    public long getNumberOfDocs() {
        return getNumberOfDocsResponse.getNumberOfDocs();
    }

    public int getSegmentCountResponseCount() {
        return getNumberOfDocsResponse.getSegmentCountResponseCount();
    }

    public List<SegmentCountResponse> getSegmentCountResponses() {
        return getNumberOfDocsResponse.getSegmentCountResponseList();
    }
}
