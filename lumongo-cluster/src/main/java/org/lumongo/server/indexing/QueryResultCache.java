package org.lumongo.server.indexing;

import org.lumongo.cluster.message.Lumongo.SegmentResponse;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class QueryResultCache {
	private Cache<QueryCacheKey, SegmentResponse> queryResultCache;
	
	public QueryResultCache(int maxSize) {
		queryResultCache = CacheBuilder.newBuilder().concurrencyLevel(16).maximumSize(maxSize).build();
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
