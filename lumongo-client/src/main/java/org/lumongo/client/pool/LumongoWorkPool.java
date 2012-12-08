package org.lumongo.client.pool;

import java.util.concurrent.Future;

import org.lumongo.client.command.BatchFetch;
import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.Delete;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.Fetch;
import org.lumongo.client.command.GetIndexes;
import org.lumongo.client.command.GetMembers;
import org.lumongo.client.command.IndexConfig;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.CreateIndexResult;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.DeleteIndexResult;
import org.lumongo.client.result.DeleteResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.client.result.StoreResult;

public class LumongoWorkPool extends LumongoBaseWorkPool {

    public LumongoWorkPool(LumongoPoolConfig lumongoPoolConfig) {
        super(lumongoPoolConfig);
    }

    public void updateMembers() throws Exception {
        GetMembersResult getMembersResult = execute(new GetMembers());
        updateMembers(getMembersResult.getMembers());
    }

    public CreateIndexResult createIndex(CreateIndex createIndex) throws Exception {
        return execute(createIndex);
    }

    public CreateIndexResult createIndex(String indexName, int segments, String uniqueIdField, IndexConfig indexConfig) throws Exception {
        return execute(new CreateIndex(indexName, 16, uniqueIdField, indexConfig));
    }

    public CreateIndexResult createIndex(String indexName, int segments, String uniqueIdField, IndexConfig indexConfig, Boolean faceted) throws Exception {
        return execute(new CreateIndex(indexName, 16, uniqueIdField, indexConfig).setFaceted(faceted));
    }

    public Future<CreateIndexResult> createIndexAsync(CreateIndex createIndex) throws Exception {
        return executeAsync(createIndex);
    }

    public Future<CreateIndexResult> createIndexAsync(String indexName, int segments, String uniqueIdField, IndexConfig indexConfig) throws Exception {
        return executeAsync(new CreateIndex(indexName, 16, uniqueIdField, indexConfig));
    }

    public Future<CreateIndexResult> createIndexAsync(String indexName, int segments, String uniqueIdField, IndexConfig indexConfig, Boolean faceted)
            throws Exception {
        return executeAsync(new CreateIndex(indexName, 16, uniqueIdField, indexConfig).setFaceted(faceted));
    }

    public StoreResult store(Store store) throws Exception {
        return execute(store);
    }

    public Future<StoreResult> storeAsync(Store store) throws Exception {
        return executeAsync(store);
    }

    public QueryResult query(Query query) throws Exception {
        return execute(query);
    }

    public Future<QueryResult> queryAsync(Query query) throws Exception {
        return executeAsync(query);
    }

    public FetchResult fetch(Fetch fetch) throws Exception {
        return execute(fetch);
    }

    public Future<FetchResult> fetchAsync(Fetch fetch) throws Exception {
        return executeAsync(fetch);
    }

    public DeleteResult delete(Delete delete) throws Exception {
        return execute(delete);
    }

    public Future<DeleteResult> deleteAsync(Delete delete) throws Exception {
        return executeAsync(delete);
    }

    public GetIndexesResult getIndexes() throws Exception {
        return execute(new GetIndexes());
    }

    public Future<GetIndexesResult> getIndexesAsync() throws Exception {
        return executeAsync(new GetIndexes());
    }

    public DeleteIndexResult deleteIndex(DeleteIndex deleteIndex) throws Exception {
        return execute(deleteIndex);
    }

    public Future<DeleteIndexResult> deleteIndexAsync(DeleteIndex deleteIndex) throws Exception {
        return executeAsync(deleteIndex);
    }

    public CreateOrUpdateIndexResult createOrUpdateIndex(CreateOrUpdateIndex createOrUpdateIndex) throws Exception {
        return execute(createOrUpdateIndex);
    }

    public Future<CreateOrUpdateIndexResult> createOrUpdateIndexAsync(CreateOrUpdateIndex createOrUpdateIndex) throws Exception {
        return executeAsync(createOrUpdateIndex);
    }

    public BatchFetchResult batchFetch(BatchFetch batchFetch) throws Exception {
        return execute(batchFetch);
    }

    public Future<BatchFetchResult> batchFetchAsync(BatchFetch batchFetch) throws Exception {
        return executeAsync(batchFetch);
    }
}

