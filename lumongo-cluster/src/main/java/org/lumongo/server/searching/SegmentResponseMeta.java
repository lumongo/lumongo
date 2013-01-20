package org.lumongo.server.searching;

import org.lumongo.cluster.message.Lumongo.SegmentResponse;

public class SegmentResponseMeta {
    private SegmentResponse segmentResponse;
    private int resultIndex;

    public SegmentResponseMeta(SegmentResponse segmentResponse, int resultIndex) {
        this.segmentResponse = segmentResponse;
        this.resultIndex = resultIndex;
    }

    public SegmentResponse getSegmentResponse() {
        return segmentResponse;
    }

    public void setSegmentResponse(SegmentResponse segmentResponse) {
        this.segmentResponse = segmentResponse;
    }

    public int getResultIndex() {
        return resultIndex;
    }

    public void setResultIndex(int resultIndex) {
        this.resultIndex = resultIndex;
    }
}
