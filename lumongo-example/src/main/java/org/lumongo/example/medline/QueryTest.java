package org.lumongo.example.medline;

import java.util.List;

import org.lumongo.client.cache.DocumentCache;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.fields.Mapper;
import org.lumongo.util.LogUtil;

public class QueryTest {
	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();
		
		LumongoWorkPool lumongoWorkPool = new LumongoWorkPool(new LumongoPoolConfig().addMember("192.168.0.1"));
		
		try {
			Mapper<Document> mapper = new Mapper<Document>(Document.class);
			
			{
				//simple query and document by document lookup
				Query query = new Query("medline", "title:cancer", 10);
				
				QueryResult queryResult = lumongoWorkPool.query(query);
				
				long totalHits = queryResult.getTotalHits();
				
				System.out.println("Found <" + totalHits + "> hits");
				for (ScoredResult sr : queryResult.getResults()) {
					
					FetchResult fr = lumongoWorkPool.fetch(new FetchDocument(sr));
					
					Document d = fr.getDocument(mapper);
					
					System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + "> <" + d.getIssn() + ">");
				}
			}
			{
				//using field sort
				Query query = new Query("medline", "title:cancer AND issn:*", 10);
				query.addFieldSort("issn");
				QueryResult queryResult = lumongoWorkPool.query(query);
				
				long totalHits = queryResult.getTotalHits();
				
				System.out.println("Found <" + totalHits + "> hits");
				for (ScoredResult sr : queryResult.getResults()) {
					
					FetchResult fr = lumongoWorkPool.fetch(new FetchDocument(sr));
					
					Document d = fr.getDocument(mapper);
					
					System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + "> <" + d.getIssn() + ">");
				}
			}
			
			{
				//using facet count
				Query query = new Query("medline", "title:cancer AND issn:*", 10);
				query.addCountRequest("issn", 4);
				QueryResult queryResult = lumongoWorkPool.query(query);
				
				long totalHits = queryResult.getTotalHits();
				
				System.out.println("Found <" + totalHits + "> hits");
				for (ScoredResult sr : queryResult.getResults()) {
					
					FetchResult fr = lumongoWorkPool.fetch(new FetchDocument(sr));
					
					Document d = fr.getDocument(mapper);
					
					System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + "> <" + d.getIssn() + ">");
				}
				
				System.out.println("Facets:");
				for (FacetCount fc : queryResult.getFacetCounts("issn")) {
					System.out.println(fc.getFacet() + ":" + fc.getCount());
				}
			}
			
			{
				//using facet count
				Query query = new Query("medline", "title:cancer AND issn:*", 10);
				query.addCountRequest("issn", 4);
				QueryResult queryResult = lumongoWorkPool.query(query);
				
				long totalHits = queryResult.getTotalHits();
				
				System.out.println("Found <" + totalHits + "> hits");
				for (ScoredResult sr : queryResult.getResults()) {
					
					FetchResult fr = lumongoWorkPool.fetch(new FetchDocument(sr));
					
					Document d = fr.getDocument(mapper);
					
					System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + "> <" + d.getIssn() + ">");
				}
				
				System.out.println("Facets:");
				for (FacetCount fc : queryResult.getFacetCounts("issn")) {
					System.out.println(fc.getFacet() + ":" + fc.getCount());
				}
			}
			
			{
				//using two facets
				Query query = new Query("medline", "title:cancer AND issn:*", 10);
				query.addCountRequest("journalCountry", 4);
				query.addCountRequest("issn", 4);
				QueryResult queryResult = lumongoWorkPool.query(query);
				
				long totalHits = queryResult.getTotalHits();
				
				System.out.println("Found <" + totalHits + "> hits");
				for (ScoredResult sr : queryResult.getResults()) {
					
					FetchResult fr = lumongoWorkPool.fetch(new FetchDocument(sr));
					
					Document d = fr.getDocument(mapper);
					
					System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + "> <" + d.getIssn() + ">");
				}
				
				System.out.println("Journal Country Facets:");
				for (FacetCount fc : queryResult.getFacetCounts("journalCountry")) {
					System.out.println(fc.getFacet() + ":" + fc.getCount());
				}
				
				System.out.println("ISSN Facets:");
				for (FacetCount fc : queryResult.getFacetCounts("issn")) {
					System.out.println(fc.getFacet() + ":" + fc.getCount());
				}
			}
			
			{
				//client side document cache
				int maxSize = 20000;
				DocumentCache documentCache = new DocumentCache(lumongoWorkPool, maxSize);
				
				{
					Query query = new Query("medline", "title:cancer AND issn:*", 10000);
					QueryResult queryResult = lumongoWorkPool.query(query);
					
					long totalHits = queryResult.getTotalHits();
					
					System.out.println("Found <" + totalHits + "> hits");
					long start = System.currentTimeMillis();
					BatchFetchResult batchFetchResult = documentCache.fetch(queryResult);
					
					long end = System.currentTimeMillis();
					System.out.println("Fetching documents took " + (end - start) + "ms");
					
					@SuppressWarnings("unused")
					List<Document> documents = mapper.fromBatchFetchResult(batchFetchResult);
				}
				
				{
					Query query = new Query("medline", "title:cancer AND issn:*", 10000);
					QueryResult queryResult = lumongoWorkPool.query(query);
					
					long totalHits = queryResult.getTotalHits();
					
					System.out.println("Found <" + totalHits + "> hits");
					long start = System.currentTimeMillis();
					BatchFetchResult batchFetchResult = documentCache.fetch(queryResult);
					
					long end = System.currentTimeMillis();
					System.out.println("Fetching documents took " + (end - start) + "ms");
					
					@SuppressWarnings("unused")
					List<Document> documents = mapper.fromBatchFetchResult(batchFetchResult);
				}
				
			}
		}
		finally {
			lumongoWorkPool.shutdown();
		}
	}
}
