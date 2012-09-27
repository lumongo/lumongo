package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.SortRequest;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class Query extends Command<QueryResult> {

    private String query;
    private int amount;
    private Collection<String> indexes;
    private Boolean realTime;
    private QueryResult lastResult;
    private FacetRequest facetRequest;
    private SortRequest sortRequest;

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

    public void setLastResult(QueryResult lastResult) {
        this.lastResult = lastResult;
    }

    public QueryResult getLastResult() {
        return lastResult;
    }

    @Override
    public QueryResult execute(LumongoConnection lumongoConnection) throws ServiceException {


        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

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
        if (facetRequest != null) {
            requestBuilder.setFacetRequest(facetRequest);
        }
        if (sortRequest != null) {
            requestBuilder.setSortRequest(sortRequest);
        }

        long start = System.currentTimeMillis();
        QueryResponse queryResponse = service.query(controller, requestBuilder.build());
        long end = System.currentTimeMillis();
        long durationInMs = end - start;
        return new QueryResult(queryResponse, durationInMs);

    }


}
