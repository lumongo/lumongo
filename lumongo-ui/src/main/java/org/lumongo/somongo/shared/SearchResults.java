package org.lumongo.somongo.shared;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.user.client.rpc.IsSerializable;

public class SearchResults implements IsSerializable {
    private List<Document> documents;

    public SearchResults() {
        documents = new ArrayList<Document>();
    }

    public List<Document> getDocuments() {
        return documents;
    }

    public void addDocument(Document d) {
        documents.add(d);
    }

}
