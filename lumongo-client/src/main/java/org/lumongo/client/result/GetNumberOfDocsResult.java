package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;

import java.util.List;

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

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("{\n  \"numberOfDocs\": ");
		sb.append(getNumberOfDocs());
		sb.append(",\n  \"segmentCounts\": [");
		for (SegmentCountResponse scr : getSegmentCountResponses()) {
			sb.append("\n    {\n      \"segmentNumber\": ");
			sb.append(scr.getSegmentNumber());
			sb.append(",\n      \"numberOfDocs\": ");
			sb.append(scr.getNumberOfDocs());
			sb.append("\n    },");
		}
		if (getSegmentCountResponseCount() != 0) {
			sb.setLength(sb.length() - 1);
		}
		sb.append("\n  ],\n  \"commandTimeMs\": ");
		sb.append(getCommandTimeMs());
		sb.append("\n}\n");
		return sb.toString();
	}

}
