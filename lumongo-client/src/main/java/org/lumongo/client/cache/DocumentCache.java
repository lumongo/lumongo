package org.lumongo.client.cache;

import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
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

    private static Cache<String, FetchResult> documentCache;

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
    public FetchResult fetch(String uniqueId) throws Exception {
        return fetch(uniqueId, null);
    }

    /**
     * Returns the last cached version of the result document if timestamp matches, fetches if the document is not in the cache or if the timestamp do not match
     * @param scoredResult - scored result returned from a search
     * @return
     * @throws Exception
     */
    public FetchResult fetch(ScoredResult scoredResult) throws Exception {
        return fetch(scoredResult.getUniqueId(), scoredResult.getTimestamp());
    }

    /**
     * Returns the last cached version of the result document if timestamp matches, fetches if the document is not in the cache or if the timestamp do not match
     * @param uniqueId - uniqueId to fetch
     * @param timestamp - timestamp to check against
     * @return
     * @throws Exception
     */
    public FetchResult fetch(String uniqueId, Long timestamp) throws Exception {
        FetchResult fr = documentCache.getIfPresent(uniqueId);

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

        if (fetch) {
            fr = lumongoWorkPool.fetch(new FetchDocument(uniqueId));
            documentCache.put(uniqueId, fr);
        }

        return fr;
    }
}
