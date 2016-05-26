package org.lumongo.server.search;

import org.apache.lucene.search.Query;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AnalyzerSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryWithFilters {
	private Query query;
	private List<Query> filterQueries = Collections.emptyList();

	private Map<String, AnalyzerSettings.Similarity> similarityOverrideMap = Collections.emptyMap();

	public QueryWithFilters(Query query) {
		this.setQuery(query);
	}
	
	public List<Query> getFilterQueries() {
		return filterQueries;
	}
	
	public void setFilterQueries(List<Query> filterQueries) {
		this.filterQueries = filterQueries;
	}
	
	public void addFilterQuery(Query filterQuery) {
		if (filterQueries.isEmpty()) {
			filterQueries = new ArrayList<>();
		}
		
		filterQueries.add(filterQuery);
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public void addSimilarityOverride(Lumongo.FieldSimilarity fieldSimilarity) {
		if (similarityOverrideMap.isEmpty()) {
			similarityOverrideMap = new HashMap<>();
		}
		similarityOverrideMap.put(fieldSimilarity.getField(), fieldSimilarity.getSimilarity());
	}

	public AnalyzerSettings.Similarity getFieldSimilarityOverride(String field) {
		AnalyzerSettings.Similarity similarity = similarityOverrideMap.get(field);
		if (similarity == null) {
			return similarityOverrideMap.get("*");
		}
		return similarity;
	}
}
