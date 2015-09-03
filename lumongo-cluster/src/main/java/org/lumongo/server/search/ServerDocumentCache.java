package org.lumongo.server.search;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.lumongo.server.index.ResultBundle;
import org.lumongo.util.LockHandler;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ServerDocumentCache {
	private Cache<String, ResultBundle> resultBundleCache;
	private LockHandler lockHandler;

	public ServerDocumentCache(int maxSize, int concurrency) {
		lockHandler = new LockHandler();
		resultBundleCache = CacheBuilder.newBuilder().concurrencyLevel(concurrency).maximumSize(maxSize).build();
	}


	public Lock getWriteLock(String uniqueId) {
		ReadWriteLock readWriteLock = lockHandler.getLock(uniqueId);
		return readWriteLock.writeLock();
	}

	public ResultBundle getFromCache(String uniqueId) {
		ReadWriteLock readWriteLock = lockHandler.getLock(uniqueId);
		readWriteLock.readLock().lock();
		try {
			return resultBundleCache.getIfPresent(uniqueId);
		}
		finally {
			readWriteLock.readLock().unlock();
		}
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
