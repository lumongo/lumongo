package org.lumongo.server.search;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.util.LockHandler;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class ServerDocumentCache {
	private Cache<String, Lumongo.ResultDocument> resultDocumentCache;
	private LockHandler lockHandler;

	public ServerDocumentCache(int maxSize, int concurrency) {
		lockHandler = new LockHandler();
		resultDocumentCache = CacheBuilder.newBuilder().concurrencyLevel(concurrency).maximumSize(maxSize).build();
	}


	public Lock getWriteLock(String uniqueId) {
		ReadWriteLock readWriteLock = lockHandler.getLock(uniqueId);
		return readWriteLock.writeLock();
	}

	public  Lumongo.ResultDocument getFromCache(String uniqueId) {
		ReadWriteLock readWriteLock = lockHandler.getLock(uniqueId);
		readWriteLock.readLock().lock();
		try {
			return resultDocumentCache.getIfPresent(uniqueId);
		}
		finally {
			readWriteLock.readLock().unlock();
		}
	}
	
	public void storeInCache(String uniqueId,  Lumongo.ResultDocument  resultDocument) {
		resultDocumentCache.put(uniqueId, resultDocument);
	}

	public void removeFromCache(String uniqueId) {
		resultDocumentCache.invalidate(uniqueId);
	}
	
	public void clear() {
		resultDocumentCache.invalidateAll();
	}
}
