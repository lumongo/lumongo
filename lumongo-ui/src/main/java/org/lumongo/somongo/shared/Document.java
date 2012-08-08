package org.lumongo.somongo.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public class Document implements IsSerializable {
    private String uniqueId;
    private int docId;
    private String indexName;
    private int segment;
    private float score;

    public Document() {

    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public int getSegment() {
        return segment;
    }

    public void setSegment(int segment) {
        this.segment = segment;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Document [uniqueId=" + uniqueId + ", docId=" + docId + ", indexName=" + indexName + ", segment=" + segment + ", score=" + score + "]";
    }


}
