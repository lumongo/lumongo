package org.lumongo.client.result;

import org.bson.Document;
import org.lumongo.cluster.message.Lumongo.AnalysisResult;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetGroup;
import org.lumongo.cluster.message.Lumongo.LastResult;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.util.ResultHelper;

import java.util.ArrayList;
import java.util.List;

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

	public ScoredResult getFirstResult() {
		if (hasResults()) {
			return getResults().get(0);
		}
		return null;
	}

	public List<Document> getDocuments() {
		List<Document> documents = new ArrayList<>();
		for (ScoredResult scoredResult : queryResponse.getResultsList()) {
			Document doc = ResultHelper.getDocumentFromScoredResult(scoredResult);
			if (doc != null) {
				documents.add(doc);
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return documents;
	}

	public Document getFirstDocument() {
		if (hasResults()) {
			Document doc = ResultHelper.getDocumentFromScoredResult(getResults().get(0));
			if (doc != null) {
				return doc;
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return null;
	}

	public LastResult getLastResult() {
		return queryResponse.getLastResult();
	}

	public List<FacetGroup> getFacetGroups() {
		return queryResponse.getFacetGroupList();
	}

	public List<FacetCount> getFacetCounts(String fieldName) {
		for (FacetGroup fg : queryResponse.getFacetGroupList()) {
			if (fieldName.equals(fg.getCountRequest().getFacetField().getLabel())) {
				return fg.getFacetCountList();
			}
		}
		return null;
	}

	public int getFacetGroupCount() {
		return queryResponse.getFacetGroupCount();
	}

	public List<AnalysisResult> getSummaryAnalysisResults() {
		return queryResponse.getAnalysisResultList();
	}

	@Override
	public String toString() {
		return queryResponse.toString();
	}

}
