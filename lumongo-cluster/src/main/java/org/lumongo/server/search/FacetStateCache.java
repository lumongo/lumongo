package org.lumongo.server.search;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexReader;
import org.lumongo.server.search.facet.LumongoSortedSetDocValuesReaderState;

import java.util.concurrent.ExecutionException;

public class FacetStateCache {
	private Cache<String, SortedSetDocValuesReaderState> queryResultCache;

	public FacetStateCache(int concurrency) {
		queryResultCache = CacheBuilder.newBuilder().concurrencyLevel(concurrency).build();
	}

	public SortedSetDocValuesReaderState getFacetState(IndexReader directoryReader, String fieldName) throws ExecutionException {
		return queryResultCache.get(fieldName, () -> new LumongoSortedSetDocValuesReaderState(directoryReader, fieldName));
	}

	public void storeInCache(String fieldName, SortedSetDocValuesReaderState sortedSetDocValuesReaderState) {
		queryResultCache.put(fieldName, sortedSetDocValuesReaderState);
	}

	public void clear() {
		queryResultCache.invalidateAll();
	}
}
