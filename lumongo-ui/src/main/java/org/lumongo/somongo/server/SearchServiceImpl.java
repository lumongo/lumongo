package org.lumongo.somongo.server;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;

import org.lumongo.client.command.GetIndexes;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.somongo.client.service.SearchService;
import org.lumongo.somongo.shared.Document;
import org.lumongo.somongo.shared.SearchRequest;
import org.lumongo.somongo.shared.SearchResults;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;



public class SearchServiceImpl extends RemoteServiceServlet implements SearchService {

	private LumongoWorkPool lumongoWorkPool;

	private static final long serialVersionUID = 1L;

	@Override
	public void init() throws ServletException {

		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();

		String lumongoServer = System.getenv("lumongoServer");

		if (lumongoServer == null) {
			lumongoServer = "localhost";
			System.err.println("--Environment variable lumongoServer is not defined, using localhost as default--");
		}

		lumongoPoolConfig.addMember(lumongoServer);
		lumongoPoolConfig.setDefaultRetries(4);

		try {
			lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public SearchResults search(SearchRequest searchRequest) throws Exception {

		String query = searchRequest.getQuery();
		int amount = searchRequest.getAmount();
		String[] indexes = searchRequest.getIndexes();
		Query q = new Query(indexes, query, amount);
		QueryResult queryResult = lumongoWorkPool.execute(q);

		SearchResults searchResults = new SearchResults();

		searchResults.setTotalHits(queryResult.getTotalHits());

		List<ScoredResult> scoredResults = queryResult.getResults();
		for (ScoredResult sr : scoredResults) {
			Document d = new Document();
			d.setUniqueId(sr.getUniqueId());
			d.setDocId(sr.getDocId());
			d.setIndexName(sr.getIndexName());
			d.setSegment(sr.getSegment());
			d.setScore(sr.getScore());
			searchResults.addDocument(d);
		}

		return searchResults;
	}

	@Override
	public List<String> getIndexes() throws Exception {

		GetIndexesResult getIndexesResult = lumongoWorkPool.execute(new GetIndexes());

		return new ArrayList<String>(getIndexesResult.getIndexNames());

	}
}
