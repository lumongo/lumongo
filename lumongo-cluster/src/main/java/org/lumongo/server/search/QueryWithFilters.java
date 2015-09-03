package org.lumongo.server.search;

import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryWithFilters {
	private Query query;
	private List<Query> filterQueries = Collections.emptyList();
	
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
			filterQueries = new ArrayList<Query>();
		}
		
		filterQueries.add(filterQuery);
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}
	
}
