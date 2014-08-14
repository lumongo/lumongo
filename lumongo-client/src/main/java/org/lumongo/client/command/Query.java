package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.FieldSort;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.LMFacet;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.SortRequest;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Runs a query on one of more LuMongo indexes.
 * @author mdavis
 *
 */
public class Query extends SimpleCommand<QueryRequest, QueryResult> {
	
	private String query;
	private int amount;
	private Collection<String> indexes;
	private Boolean realTime;
	private QueryResult lastResult;
	private List<CountRequest> countRequests = Collections.emptyList();
	private List<LMFacet> drillDowns = Collections.emptyList();
	private List<FieldSort> fieldSorts = Collections.emptyList();
	private Set<String> queryFields = Collections.emptySet();
	private List<String> filterQueries = Collections.emptyList();

	private Boolean drillSideways;
	
	public Query(String index, String query, int amount) {
		this(new String[] { index }, query, amount);
	}
	
	public Query(String[] indexes, String query, int amount) {
		this(new ArrayList<String>(Arrays.asList(indexes)), query, amount);
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
	
	public Query setIndexes(Collection<String> indexes) {
		this.indexes = indexes;
		return this;
	}
	
	public Collection<String> getIndexes() {
		return indexes;
	}
	
	public Query setRealTime(Boolean realTime) {
		this.realTime = realTime;
		return this;
	}
	
	public Boolean getRealTime() {
		return realTime;
	}
	
	public Boolean getDrillSideways() {
		return drillSideways;
	}
	
	public Query setDrillSideways(Boolean drillSideways) {
		this.drillSideways = drillSideways;
		return this;
	}
	
	public Query setLastResult(QueryResult lastResult) {
		this.lastResult = lastResult;
		return this;
	}
	
	public QueryResult getLastResult() {
		return lastResult;
	}
	
	public Query addDrillDown(String label, String... path) {
		if (drillDowns.isEmpty()) {
			this.drillDowns = new ArrayList<LMFacet>();
		}
		
		drillDowns.add(LMFacet.newBuilder().setLabel(label).addAllPath(Arrays.asList(path)).build());
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
		this.queryFields = new HashSet<String>(Arrays.asList(queryFields));
		
	}
	
	public Query addQueryField(String queryField) {
		if (queryFields.isEmpty()) {
			this.queryFields = new HashSet<String>();
		}
		
		queryFields.add(queryField);
		return this;
	}
	
	public Query addQueryField(String... queryFields) {
		if (this.queryFields.isEmpty()) {
			this.queryFields = new HashSet<String>();
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
			this.filterQueries = new ArrayList<String>();
		}
		
		filterQueries.add(filterQuery);
		return this;
	}

	public Query addCountRequest(String label, String... path) {
		LMFacet facet = LMFacet.newBuilder().setLabel(label).addAllPath(Arrays.asList(path)).build();
		CountRequest countRequest = CountRequest.newBuilder().setFacetField(facet).build();
		if (countRequests.isEmpty()) {
			this.countRequests = new ArrayList<CountRequest>();
		}
		countRequests.add(countRequest);
		return this;
	}
	
	public Query addCountRequest(int maxFacets, String label, String... path) {
		LMFacet facet = LMFacet.newBuilder().setLabel(label).addAllPath(Arrays.asList(path)).build();
		CountRequest countRequest = CountRequest.newBuilder().setFacetField(facet).setMaxFacets(maxFacets).build();
		if (countRequests.isEmpty()) {
			this.countRequests = new ArrayList<CountRequest>();
		}
		countRequests.add(countRequest);
		return this;
	}
	
	public List<CountRequest> getCountRequests() {
		return countRequests;
	}
	
	public void addFieldSort(String sort) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<FieldSort>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(Direction.ASCENDING).build());
	}
	
	public void addFieldSort(String sort, Direction direction) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<FieldSort>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(direction).build());
	}
	
	@Override
	public QueryRequest getRequest() {
		QueryRequest.Builder requestBuilder = QueryRequest.newBuilder();
		requestBuilder.setAmount(amount);
		requestBuilder.setQuery(query);
		if (realTime != null) {
			requestBuilder.setRealTime(realTime);
		}
		if (lastResult != null) {
			requestBuilder.setLastResult(lastResult.getLastResult());
		}
		
		for (String index : indexes) {
			requestBuilder.addIndex(index);
		}
		
		if (!drillDowns.isEmpty() || !countRequests.isEmpty()) {
			FacetRequest.Builder facetRequestBuilder = FacetRequest.newBuilder();
			
			if (drillSideways != null) {
				facetRequestBuilder.setDrillSideways(drillSideways);
			}
			
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
	
}
