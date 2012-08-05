package org.lumongo.somongo.shared;

import java.util.List;

import com.google.gwt.user.client.rpc.IsSerializable;

public class SearchRequest implements IsSerializable {

    private String[] indexes;
    private String query;
    private int amount;

    public SearchRequest() {

    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String[] getIndexes() {
        return indexes;
    }

    public void setIndexes(String[] indexes) {
        this.indexes = indexes;
    }

    public void setIndexes(List<String> indexes) {
        this.indexes = indexes.toArray(new String[0]);
    }

}
