package org.lumongo.somongo.client.service;

import java.util.List;

import org.lumongo.somongo.shared.SearchRequest;
import org.lumongo.somongo.shared.SearchResults;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface SearchServiceAsync {

    void search(SearchRequest searchRequest, AsyncCallback<SearchResults> callback);

    void getIndexes(AsyncCallback<List<String>> callback);

}
