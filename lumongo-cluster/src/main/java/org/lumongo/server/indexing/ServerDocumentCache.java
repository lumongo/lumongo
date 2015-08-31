package org.lumongo.server.indexing;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ServerDocumentCache {
	private Cache<String, ResultBundle> resultBundleCache;

	public ServerDocumentCache(int maxSize, int concurrency) {
		resultBundleCache = CacheBuilder.newBuilder().concurrencyLevel(concurrency).maximumSize(maxSize).build();
	}
	
	public ResultBundle getFromCache(String uniqueId) {
		return resultBundleCache.getIfPresent(uniqueId);
	}
	
	public void storeInCache(String uniqueId, ResultBundle resultBundle) {
		resultBundleCache.put(uniqueId, resultBundle);
	}

	public void removeFromCache(String uniqueId) {
		resultBundleCache.invalidate(uniqueId);
	}
	
	public void clear() {
		resultBundleCache.invalidateAll();
	}
}
