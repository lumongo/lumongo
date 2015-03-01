package org.lumongo.example.wikipedia;

import org.lumongo.client.cache.DocumentCache;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.fields.Mapper;
import org.lumongo.util.LogUtil;

import java.util.List;

public class SearchWikipedia {
	private static LumongoWorkPool lumongoWorkPool;
	private static Mapper<Article> mapper;

	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();

		lumongoWorkPool = new LumongoWorkPool(new LumongoPoolConfig().addMember("localhost"));
		mapper = new Mapper<Article>(Article.class);

		int maxSize = 2000;
		DocumentCache documentCache = new DocumentCache(lumongoWorkPool, maxSize);

		Query query = new Query("wikipedia", "title:a*", 10);
		QueryResult queryResult = lumongoWorkPool.query(query);

		BatchFetchResult batchFetchResult = documentCache.fetch(queryResult);

		List<Article> articles = mapper.fromBatchFetchResult(batchFetchResult);

		System.out.println(articles);

		lumongoWorkPool.shutdown();
	}
}
