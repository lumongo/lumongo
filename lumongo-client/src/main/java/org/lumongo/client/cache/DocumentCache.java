package org.lumongo.client.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.lumongo.client.command.BatchFetch;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Caches lumongo result documents.  Timestamp associated with the result document can be used to match searches with result documents
 * @author mdavis
 *
 */
public class DocumentCache {
	private LumongoWorkPool lumongoWorkPool;
	
	private static Cache<DocId, FetchResult> documentCache;
	
	public DocumentCache(LumongoWorkPool lumongoWorkPool, int maxSize) {
		this.lumongoWorkPool = lumongoWorkPool;
		documentCache = CacheBuilder.newBuilder().concurrencyLevel(16).maximumSize(maxSize).build();
	}
	
	/**
	 * Returns the last cached version of the result document or fetches if the result document is not in the cache
	 * @param uniqueId - uniqueId to fetch
	 * @return
	 * @throws Exception
	 */
	public FetchResult fetch(String uniqueId, String indexName) throws Exception {
		return fetch(uniqueId, indexName, null);
	}
	
	/**
	 * Returns the last cached version of the result document if timestamp matches, fetches if the document is not in the cache or if the timestamp do not match
	 * @param scoredResult - scored result returned from a search
	 * @return
	 * @throws Exception
	 */
	public FetchResult fetch(ScoredResult scoredResult) throws Exception {
		return fetch(scoredResult.getUniqueId(), scoredResult.getIndexName(), scoredResult.getTimestamp());
	}
	
	/**
	 * Returns the last cached version of the result document if timestamp matches, fetches if the document is not in the cache or if the timestamp do not match
	 * @param uniqueId - uniqueId to fetch
	 * @param timestamp - timestamp to check against
	 * @return
	 * @throws Exception
	 */
	public FetchResult fetch(String uniqueId, String indexName, Long timestamp) throws Exception {
		FetchResult fr = documentCache.getIfPresent(new DocId(uniqueId, indexName));
		
		boolean fetch = fetchNeeded(fr, timestamp);
		
		if (fetch) {
			fr = lumongoWorkPool.fetch(new FetchDocument(uniqueId, indexName));
			if (fr.hasResultDocument()) {
				documentCache.put(new DocId(uniqueId, indexName), fr);
				
			}
		}
		
		return fr;
	}
	
	public BatchFetchResult fetch(QueryResult queryResult) throws Exception {
		return fetch(queryResult.getResults());
	}
	
	public BatchFetchResult fetch(Collection<ScoredResult> scoredResults) throws Exception {
		
		List<FetchResult> resultsFromCache = new ArrayList<FetchResult>();
		List<FetchDocument> fetchDocumentList = new ArrayList<FetchDocument>();
		
		for (ScoredResult sr : scoredResults) {
			FetchResult fetchResult = documentCache.getIfPresent(new DocId(sr.getUniqueId(), sr.getIndexName()));
			boolean fetch = fetchNeeded(fetchResult, sr.getTimestamp());
			
			if (fetch) {
				fetchDocumentList.add(new FetchDocument(sr.getUniqueId(), sr.getIndexName()));
			}
			else {
				resultsFromCache.add(fetchResult);
			}
		}
		
		if (!fetchDocumentList.isEmpty()) {
			BatchFetchResult bfr = lumongoWorkPool.batchFetch(new BatchFetch().addFetches(fetchDocumentList));
			for (FetchResult fr : bfr.getFetchResults()) {
				if (fr.hasResultDocument()) {
					documentCache.put(new DocId(fr.getUniqueId(), fr.getIndexName()), fr);
				}
			}
			
			bfr.getFetchResults().addAll(resultsFromCache);
			return bfr;
		}
		else {
			return new BatchFetchResult(resultsFromCache);
		}
		
	}
	
	private boolean fetchNeeded(FetchResult fr, Long timestamp) {
		boolean fetch = false;
		
		if (fr == null) { //no result in cache - fetch regardless of passed time stamp
			fetch = true;
		}
		else if (timestamp != null) { //asking for a specific version and found a version in cache
			if (fr.getDocumentTimestamp() == null) { //no document in cache and asking for document with a timestamp
				fetch = true;
			}
			else if (Long.compare(fr.getDocumentTimestamp(), timestamp) != 0) { //outdated document in cache
				fetch = true;
			}
		}
		return fetch;
	}
}
