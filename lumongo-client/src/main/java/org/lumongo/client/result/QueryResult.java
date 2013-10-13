package org.lumongo.client.result;

import java.util.List;

import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetGroup;
import org.lumongo.cluster.message.Lumongo.LastResult;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

public class QueryResult extends Result {
	private QueryResponse queryResponse;
	
	public QueryResult(QueryResponse queryResponse) {
		this.queryResponse = queryResponse;
	}
	
	public long getTotalHits() {
		return queryResponse.getTotalHits();
	}
	
	public boolean hasResults() {
		return !queryResponse.getResultsList().isEmpty();
	}
	
	public List<ScoredResult> getResults() {
		return queryResponse.getResultsList();
	}
	
	public LastResult getLastResult() {
		return queryResponse.getLastResult();
	}
	
	public List<FacetGroup> getFacetGroups() {
		return queryResponse.getFacetGroupList();
	}
	
	public List<FacetCount> getFacetCounts(String fieldName) {
		for (FacetGroup fg : queryResponse.getFacetGroupList()) {
			if (fieldName.equals(fg.getFieldName())) {
				return fg.getFacetCountList();
			}
		}
		return null;
	}
	
	public int getFacetGroupCount() {
		return queryResponse.getFacetGroupCount();
	}
	
	@Override
	public String toString() {
		return queryResponse.toString();
	}
	
}
