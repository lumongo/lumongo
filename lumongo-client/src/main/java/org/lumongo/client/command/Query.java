package org.lumongo.client.command;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.FieldSort;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.LMFacet;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryRequest.Operator;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.SortRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Runs a query on one of more LuMongo indexes.
 * @author mdavis
 *
 */
public class Query extends SimpleCommand<QueryRequest, QueryResult> {
	
	private String query;
	private int amount;
	private Collection<String> indexes;
	private Lumongo.LastResult lastResult;
	private List<CountRequest> countRequests = Collections.emptyList();
	private List<LMFacet> drillDowns = Collections.emptyList();
	private List<FieldSort> fieldSorts = Collections.emptyList();
	private Set<String> queryFields = Collections.emptySet();
	private List<String> filterQueries = Collections.emptyList();
	private Integer minimumNumberShouldMatch;
	private Operator defaultOperator;
	private Lumongo.FetchType resultFetchType;
	private Set<String> documentFields = Collections.emptySet();
	private Set<String> documentMaskedFields = Collections.emptySet();


	public Query(String index, String query, int amount) {
		this(new String[] { index }, query, amount);
	}
	
	public Query(String[] indexes, String query, int amount) {
		this(new ArrayList<>(Arrays.asList(indexes)), query, amount);
	}
	
	public Query(Collection<String> indexes, String query, int amount) {
		this.indexes = indexes;
		this.query = query;
		this.amount = amount;
	}
	
	public String getQuery() {
		return query;
	}
	
	public Query setQuery(String query) {
		this.query = query;
		return this;
	}
	
	public int getAmount() {
		return amount;
	}
	
	public Query setAmount(int amount) {
		this.amount = amount;
		return this;
	}
	
	public Integer getMinimumNumberShouldMatch() {
		return minimumNumberShouldMatch;
	}
	
	public void setMinimumNumberShouldMatch(Integer minimumNumberShouldMatch) {
		this.minimumNumberShouldMatch = minimumNumberShouldMatch;
	}
	
	public Query setIndexes(Collection<String> indexes) {
		this.indexes = indexes;
		return this;
	}
	
	public Collection<String> getIndexes() {
		return indexes;
	}


	public Query setLastResult(QueryResult lastQueryResult) {
		this.lastResult = lastQueryResult.getLastResult();
		return this;
	}

	public Query setLastResult(Lumongo.LastResult lastResult) {
		this.lastResult = lastResult;
		return this;
	}
	
	public Lumongo.LastResult getLastResult() {
		return lastResult;
	}
	
	public Query addDrillDown(String label, String path) {
		if (drillDowns.isEmpty()) {
			this.drillDowns = new ArrayList<>();
		}
		
		drillDowns.add(LMFacet.newBuilder().setLabel(label).setPath(path).build());
		return this;
	}
	
	public List<LMFacet> getDrillDowns() {
		return drillDowns;
	}
	
	public Set<String> getQueryFields() {
		return queryFields;
	}
	
	public void setQueryFields(Collection<String> queryFields) {
		this.queryFields = new HashSet<String>(queryFields);
	}
	
	public void setQueryFields(String... queryFields) {
		this.queryFields = new HashSet<>(Arrays.asList(queryFields));
		
	}
	
	public Query addQueryField(String queryField) {
		if (queryFields.isEmpty()) {
			this.queryFields = new HashSet<>();
		}
		
		queryFields.add(queryField);
		return this;
	}
	
	public Query addQueryField(String... queryFields) {
		if (this.queryFields.isEmpty()) {
			this.queryFields = new HashSet<>();
		}
		
		for (String queryField : queryFields) {
			this.queryFields.add(queryField);
		}
		return this;
	}
	
	public List<String> getFilterQueries() {
		return filterQueries;
	}
	
	public void setFilterQueries(List<String> filterQueries) {
		this.filterQueries = filterQueries;
	}
	
	public Query addFilterQuery(String filterQuery) {
		if (filterQueries.isEmpty()) {
			this.filterQueries = new ArrayList<>();
		}
		
		filterQueries.add(filterQuery);
		return this;
	}

	public Query addCountRequest(String label) {
		return(addCountRequest(label, 10));
	}

	public Query addCountRequest(String label, int maxFacets) {
		return(addCountRequest(label, maxFacets, maxFacets * 8));
	}

	public Query addCountRequest(String label, int maxFacets, int segmentFacets) {

		LMFacet facet = LMFacet.newBuilder().setLabel(label).build();
		CountRequest countRequest = CountRequest.newBuilder().setFacetField(facet).setMaxFacets(maxFacets).setSegmentFacets(segmentFacets).build();
		if (countRequests.isEmpty()) {
			this.countRequests = new ArrayList<>();
		}
		countRequests.add(countRequest);
		return this;
	}

	public List<CountRequest> getCountRequests() {
		return countRequests;
	}
	
	public Query addFieldSort(String sort) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(Direction.ASCENDING).build());
		return this;
	}
	
	public Query addFieldSort(String sort, Direction direction) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(direction).build());
		return this;
	}
	
	public Operator getDefaultOperator() {
		return defaultOperator;
	}
	
	public Query setDefaultOperator(Operator defaultOperator) {
		this.defaultOperator = defaultOperator;
		return this;
	}

	public Lumongo.FetchType getResultFetchType() {
		return resultFetchType;
	}

	public Query setResultFetchType(Lumongo.FetchType resultFetchType) {
		this.resultFetchType = resultFetchType;
		return this;
	}

	public Set<String> getDocumentMaskedFields() {
		return documentMaskedFields;
	}

	public Query addDocumentMaskedField(String documentMaskedField) {
		if (documentMaskedFields.isEmpty()) {
			documentMaskedFields = new LinkedHashSet<>();
		}

		documentMaskedFields.add(documentMaskedField);
		return this;
	}

	public Set<String> getDocumentFields() {
		return documentFields;
	}

	public Query addDocumentField(String documentField) {
		if (documentFields.isEmpty()) {
			this.documentFields = new LinkedHashSet<>();
		}
		documentFields.add(documentField);
		return this;
	}

	@Override
	public QueryRequest getRequest() {
		QueryRequest.Builder requestBuilder = QueryRequest.newBuilder();
		requestBuilder.setAmount(amount);
		
		if (minimumNumberShouldMatch != null) {
			requestBuilder.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
		}
		
		if (query != null) {
			requestBuilder.setQuery(query);
		}

		if (lastResult != null) {
			requestBuilder.setLastResult(lastResult);
		}
		
		for (String index : indexes) {
			requestBuilder.addIndex(index);
		}
		
		if (!drillDowns.isEmpty() || !countRequests.isEmpty()) {
			FacetRequest.Builder facetRequestBuilder = FacetRequest.newBuilder();

			facetRequestBuilder.addAllDrillDown(drillDowns);
			
			facetRequestBuilder.addAllCountRequest(countRequests);
			
			requestBuilder.setFacetRequest(facetRequestBuilder.build());
			
		}
		
		if (!queryFields.isEmpty()) {
			requestBuilder.addAllQueryField(queryFields);
		}
		
		if (!filterQueries.isEmpty()) {
			requestBuilder.addAllFilterQuery(filterQueries);
		}
		
		if (defaultOperator != null) {
			requestBuilder.setDefaultOperator(defaultOperator);
		}

		if (resultFetchType != null) {
			requestBuilder.setResultFetchType(resultFetchType);
		}

		requestBuilder.addAllDocumentFields(documentFields);
		requestBuilder.addAllDocumentMaskedFields(documentMaskedFields);
		
		SortRequest.Builder sortRequestBuilder = SortRequest.newBuilder();
		sortRequestBuilder.addAllFieldSort(fieldSorts);
		requestBuilder.setSortRequest(sortRequestBuilder.build());
		
		return requestBuilder.build();
	}
	
	@Override
	public QueryResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		
		ExternalService.BlockingInterface service = lumongoConnection.getService();
		
		RpcController controller = lumongoConnection.getController();
		
		QueryResponse queryResponse = service.query(controller, getRequest());
		
		return new QueryResult(queryResponse);
		
	}

	@Override
	public String toString() {
		return "Query{" +
				"query='" + query + '\'' +
				", amount=" + amount +
				", indexes=" + indexes +
				", lastResult=" + lastResult +
				", countRequests=" + countRequests +
				", drillDowns=" + drillDowns +
				", fieldSorts=" + fieldSorts +
				", queryFields=" + queryFields +
				", filterQueries=" + filterQueries +
				", minimumNumberShouldMatch=" + minimumNumberShouldMatch +
				", defaultOperator=" + defaultOperator +
				", resultFetchType=" + resultFetchType +
				", documentFields=" + documentFields +
				", documentMaskedFields=" + documentMaskedFields +
				'}';
	}
}
