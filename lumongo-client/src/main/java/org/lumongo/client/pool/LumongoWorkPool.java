package org.lumongo.client.pool;

import org.lumongo.client.command.BatchFetch;
import org.lumongo.client.command.ClearIndex;
import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.Delete;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.Fetch;
import org.lumongo.client.command.FetchLargeAssociated;
import org.lumongo.client.command.GetAllTerms;
import org.lumongo.client.command.GetFields;
import org.lumongo.client.command.GetIndexes;
import org.lumongo.client.command.GetMembers;
import org.lumongo.client.command.GetNumberOfDocs;
import org.lumongo.client.command.GetTerms;
import org.lumongo.client.command.OptimizeIndex;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.command.StoreLargeAssociated;
import org.lumongo.client.command.UpdateIndex;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.ClearIndexResult;
import org.lumongo.client.result.CreateIndexResult;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.DeleteIndexResult;
import org.lumongo.client.result.DeleteResult;
import org.lumongo.client.result.FetchLargeAssociatedResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetFieldsResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.client.result.GetNumberOfDocsResult;
import org.lumongo.client.result.GetTermsResult;
import org.lumongo.client.result.OptimizeIndexResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.client.result.StoreLargeAssociatedResult;
import org.lumongo.client.result.StoreResult;
import org.lumongo.client.result.UpdateIndexResult;

import com.google.common.util.concurrent.ListenableFuture;

public class LumongoWorkPool extends LumongoBaseWorkPool {
	
	public LumongoWorkPool(LumongoPoolConfig lumongoPoolConfig) throws Exception {
		super(lumongoPoolConfig);
	}
	
	public BatchFetchResult batchFetch(BatchFetch batchFetch) throws Exception {
		return execute(batchFetch);
	}
	
	public ListenableFuture<BatchFetchResult> batchFetchAsync(BatchFetch batchFetch) throws Exception {
		return executeAsync(batchFetch);
	}
	
	public ClearIndexResult clearIndex(ClearIndex clearIndex) throws Exception {
		return execute(clearIndex);
	}
	
	public ListenableFuture<ClearIndexResult> clearIndexAsync(ClearIndex clearIndex) throws Exception {
		return executeAsync(clearIndex);
	}
	
	public CreateIndexResult createIndex(CreateIndex createIndex) throws Exception {
		return execute(createIndex);
	}
	
	public CreateIndexResult createIndex(String indexName, int segments, String uniqueIdField, IndexConfig indexConfig) throws Exception {
		return execute(new CreateIndex(indexName, segments, uniqueIdField, indexConfig));
	}
	
	public ListenableFuture<CreateIndexResult> createIndexAsync(CreateIndex createIndex) throws Exception {
		return executeAsync(createIndex);
	}
	
	public ListenableFuture<CreateIndexResult> createIndexAsync(String indexName, int segments, String uniqueIdField, IndexConfig indexConfig) throws Exception {
		return executeAsync(new CreateIndex(indexName, segments, uniqueIdField, indexConfig));
	}
	
	public CreateOrUpdateIndexResult createOrUpdateIndex(CreateOrUpdateIndex createOrUpdateIndex) throws Exception {
		return execute(createOrUpdateIndex);
	}
	
	public ListenableFuture<CreateOrUpdateIndexResult> createOrUpdateIndexAsync(CreateOrUpdateIndex createOrUpdateIndex) throws Exception {
		return executeAsync(createOrUpdateIndex);
	}
	
	public DeleteResult delete(Delete delete) throws Exception {
		return execute(delete);
	}
	
	public ListenableFuture<DeleteResult> deleteAsync(Delete delete) throws Exception {
		return executeAsync(delete);
	}
	
	public DeleteIndexResult deleteIndex(String indexName) throws Exception {
		return execute(new DeleteIndex(indexName));
	}
	
	public ListenableFuture<DeleteIndexResult> deleteIndexAsync(String indexName) throws Exception {
		return executeAsync(new DeleteIndex(indexName));
	}
	
	public DeleteIndexResult deleteIndex(DeleteIndex deleteIndex) throws Exception {
		return execute(deleteIndex);
	}
	
	public ListenableFuture<DeleteIndexResult> deleteIndexAsync(DeleteIndex deleteIndex) throws Exception {
		return executeAsync(deleteIndex);
	}
	
	public FetchResult fetch(Fetch fetch) throws Exception {
		return execute(fetch);
	}
	
	public ListenableFuture<FetchResult> fetchAsync(Fetch fetch) throws Exception {
		return executeAsync(fetch);
	}
	
	public FetchLargeAssociatedResult fetchLargeAssociated(FetchLargeAssociated fetchLargeAssociated) throws Exception {
		return execute(fetchLargeAssociated);
	}
	
	public ListenableFuture<FetchLargeAssociatedResult> fetchLargeAssociatedAsync(FetchLargeAssociated fetchLargeAssociated) throws Exception {
		return executeAsync(fetchLargeAssociated);
	}
	
	public GetTermsResult getAllTerms(GetAllTerms getTerms) throws Exception {
		return execute(getTerms);
	}
	
	public ListenableFuture<GetTermsResult> getAllTermsAsync(GetAllTerms getTerms) throws Exception {
		return executeAsync(getTerms);
	}
	
	public GetFieldsResult getFields(GetFields getFields) throws Exception {
		return execute(getFields);
	}
	
	public ListenableFuture<GetFieldsResult> getFieldsAsync(GetFields getFields) throws Exception {
		return executeAsync(getFields);
	}
	
	public GetIndexesResult getIndexes() throws Exception {
		return execute(new GetIndexes());
	}
	
	public ListenableFuture<GetIndexesResult> getIndexesAsync() throws Exception {
		return executeAsync(new GetIndexes());
	}
	
	public GetMembersResult getMembers() throws Exception {
		return execute(new GetMembers());
	}
	
	public ListenableFuture<GetMembersResult> getMembersAsync() throws Exception {
		return executeAsync(new GetMembers());
	}
	
	public GetNumberOfDocsResult getNumberOfDocs(String indexName) throws Exception {
		return getNumberOfDocs(new GetNumberOfDocs(indexName));
	}
	
	public GetNumberOfDocsResult getNumberOfDocs(GetNumberOfDocs getNumberOfDocs) throws Exception {
		return execute(getNumberOfDocs);
	}
	
	public ListenableFuture<GetNumberOfDocsResult> getNumberOfDocsAsync(String indexName) throws Exception {
		return executeAsync(new GetNumberOfDocs(indexName));
	}
	
	public ListenableFuture<GetNumberOfDocsResult> getNumberOfDocsAsync(GetNumberOfDocs getNumberOfDocs) throws Exception {
		return executeAsync(getNumberOfDocs);
	}
	
	public GetTermsResult getTerms(GetTerms getTerms) throws Exception {
		return execute(getTerms);
	}
	
	public ListenableFuture<GetTermsResult> getTermsAsync(GetTerms getTerms) throws Exception {
		return executeAsync(getTerms);
	}
	
	public OptimizeIndexResult optimizeIndex(OptimizeIndex optimizeIndex) throws Exception {
		return execute(optimizeIndex);
	}
	
	public ListenableFuture<OptimizeIndexResult> optimizeIndexAsync(OptimizeIndex optimizeIndex) throws Exception {
		return executeAsync(optimizeIndex);
	}
	
	public QueryResult query(Query query) throws Exception {
		return execute(query);
	}
	
	public ListenableFuture<QueryResult> queryAsync(Query query) throws Exception {
		return executeAsync(query);
	}
	
	public StoreResult store(Store store) throws Exception {
		return execute(store);
	}
	
	public ListenableFuture<StoreResult> storeAsync(Store store) throws Exception {
		return executeAsync(store);
	}
	
	public StoreLargeAssociatedResult storeLargeAssociated(StoreLargeAssociated storeLargeAssociated) throws Exception {
		return execute(storeLargeAssociated);
	}
	
	public ListenableFuture<StoreLargeAssociatedResult> storeLargeAssociatedAsync(StoreLargeAssociated storeLargeAssociated) throws Exception {
		return executeAsync(storeLargeAssociated);
	}
	
	public UpdateIndexResult updateIndex(UpdateIndex updateIndex) throws Exception {
		return execute(updateIndex);
	}
	
	public ListenableFuture<UpdateIndexResult> updateIndexAsync(UpdateIndex updateIndex) throws Exception {
		return executeAsync(updateIndex);
	}
	
	public void updateMembers() throws Exception {
		GetMembersResult getMembersResult = execute(new GetMembers());
		updateMembers(getMembersResult.getMembers());
	}
}
