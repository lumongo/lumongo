package org.lumongo.somongo.client.service;

import org.lumongo.somongo.shared.SearchRequest;
import org.lumongo.somongo.shared.SearchResults;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface SearchServiceAsync {

    void search(SearchRequest searchRequest, AsyncCallback<SearchResults> callback);

}
