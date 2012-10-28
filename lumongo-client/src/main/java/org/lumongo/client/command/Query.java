package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.FieldSort;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
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
    private List<CountRequest> countRequests;
    private List<String> drillDowns;
    private List<FieldSort> fieldSorts;

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
        this.countRequests = new ArrayList<CountRequest>();
        this.drillDowns = new ArrayList<String>();
        this.fieldSorts = new ArrayList<FieldSort>();
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

    public Query setLastResult(QueryResult lastResult) {
        this.lastResult = lastResult;
        return this;
    }

    public QueryResult getLastResult() {
        return lastResult;
    }

    public Query addDrillDown(String drillDown) {
        drillDowns.add(drillDown);
        return this;
    }

    public List<String> getDrillDowns() {
        return drillDowns;
    }

    public Query addCountRequest(String facet) {
        CountRequest countRequest = CountRequest.newBuilder().setFacet(facet).build();
        countRequests.add(countRequest);
        return this;
    }

    public Query addCountRequest(String facet, int maxFacets) {
        CountRequest countRequest = CountRequest.newBuilder().setFacet(facet).setMaxFacets(maxFacets).build();
        countRequests.add(countRequest);
        return this;
    }

    public List<CountRequest> getCountRequests() {
        return countRequests;
    }

    public void addFieldSort(String sort) {
        fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(Direction.ASCENDING).build());
    }

    public void addFieldSort(String sort, Direction direction) {
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

            facetRequestBuilder.addAllDrillDown(drillDowns);

            facetRequestBuilder.addAllCountRequest(countRequests);

            requestBuilder.setFacetRequest(facetRequestBuilder.build());
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
