package org.lumongo.somongo.shared;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.user.client.rpc.IsSerializable;

public class SearchResults implements IsSerializable {

    private long totalHits;
    private List<Document> documents;

    public SearchResults() {
        documents = new ArrayList<Document>();
    }

    public long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(long totalHits) {
        this.totalHits = totalHits;
    }

    public List<Document> getDocuments() {
        return documents;
    }

    public void addDocument(Document d) {
        documents.add(d);
    }

    @Override
    public String toString() {
        return "SearchResults [totalHits=" + totalHits + ", documents=" + documents + "]";
    }

}
