package org.lumongo.somongo.client.service;

import java.util.List;

import org.lumongo.somongo.shared.SearchRequest;
import org.lumongo.somongo.shared.SearchResults;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

@RemoteServiceRelativePath("search")
public interface SearchService extends RemoteService {
    public SearchResults search(SearchRequest searchRequest) throws Exception;

    public List<String> getIndexes() throws Exception;
}
