package org.lumongo.server.search;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;

public class QueryResultCache {
	private Cache<QueryCacheKey, SegmentResponse> queryResultCache;
	
	public QueryResultCache(int maxSize, int concurrency) {
		queryResultCache = CacheBuilder.newBuilder().concurrencyLevel(concurrency).maximumSize(maxSize).build();
	}
	
	public SegmentResponse getCacheSegmentResponse(QueryCacheKey queryCacheKey) {
		return queryResultCache.getIfPresent(queryCacheKey);
	}
	
	public void storeInCache(QueryCacheKey queryCacheKey, SegmentResponse segmentResponse) {
		queryResultCache.put(queryCacheKey, segmentResponse);
	}
	
	public void clear() {
		queryResultCache.invalidateAll();
	}
}
