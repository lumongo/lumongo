package org.lumongo.somongo.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;

import org.lumongo.client.LumongoClient;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo.GetIndexesResponse;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.somongo.client.service.SearchService;
import org.lumongo.somongo.shared.Document;
import org.lumongo.somongo.shared.SearchRequest;
import org.lumongo.somongo.shared.SearchResults;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;



public class SearchServiceImpl extends RemoteServiceServlet implements SearchService {

    private LumongoClient lumongoClient;

    private static final long serialVersionUID = 1L;

    @Override
    public void init() throws ServletException {
        // TODO make configurable
        LumongoClientConfig lumongoClientConfig = new LumongoClientConfig();
        lumongoClientConfig.addMember("192.168.0.1");
        lumongoClientConfig.setDefaultRetries(4);
        try {
            lumongoClient = new LumongoClient(lumongoClientConfig);
        } catch (IOException e) {
            throw new ServletException(e);
        }
    }

    @Override
    public SearchResults search(SearchRequest searchRequest) throws Exception {

        String query = searchRequest.getQuery();
        int amount = searchRequest.getAmount();
        String[] indexes = searchRequest.getIndexes();
        QueryResponse queryResponse = lumongoClient.query(query, amount, indexes);

        SearchResults searchResults = new SearchResults();

        searchResults.setTotalHits(queryResponse.getTotalHits());

        List<ScoredResult> scoredResults = queryResponse.getResultsList();
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

        GetIndexesResponse getIndexesResponse = lumongoClient.getIndexes();

        return new ArrayList<String>(getIndexesResponse.getIndexNameList());

    }
}
