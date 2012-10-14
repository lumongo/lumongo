package org.lumongo.client.result;

import java.util.List;

import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.util.BsonHelper;

import com.google.protobuf.ByteString;
import com.mongodb.DBObject;

public class FetchResult extends Result {

    private FetchResponse fetchResponse;

    public FetchResult(FetchResponse fetchResponse) {
        this.fetchResponse = fetchResponse;
    }

    public ResultDocument getResultDocument() {
        return fetchResponse.getResultDocument();
    }

    public ResultDocument.Type getDocumentType() {
        if (!fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            return rd.getType();
        }
        return null;
    }

    public byte[] getDocumentAsBytes() {
        if (!fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            ByteString contents = rd.toByteString();
            return contents.toByteArray();
        }
        return null;
    }

    public String getDocumentAsUtf8() {
        if (!fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            String contents = rd.toByteString().toStringUtf8();
            return contents;
        }
        return null;
    }

    public DBObject getDocumentAsBson() {
        if (!fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            DBObject dbObject = BsonHelper.dbObjectFromResultDocument(rd);
            return dbObject;
        }
        return null;
    }

    public List<AssociatedDocument> getAssociatedDocuments() {
        return fetchResponse.getAssociatedDocumentList();
    }


}
