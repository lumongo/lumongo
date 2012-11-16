package org.lumongo.client.result;

import java.util.List;

import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.fields.Mapper;
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

    public String getUniqueId() {
        if (fetchResponse.hasResultDocument()) {
            return fetchResponse.getResultDocument().getUniqueId();
        }
        return null;
    }

    public ResultDocument.Type getDocumentType() {
        if (fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            return rd.getType();
        }
        return null;
    }

    public boolean isDocumentBson() {
        return isDocumentType(ResultDocument.Type.BSON);
    }

    public boolean isDocumentBinary() {
        return isDocumentType(ResultDocument.Type.BINARY);
    }

    public boolean isDocumentText() {
        return isDocumentType(ResultDocument.Type.TEXT);
    }

    public boolean isDocumentType(ResultDocument.Type type) {
        boolean retVal = false;
        if (fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            retVal = (type.equals(rd.getType()));
        }
        return retVal;
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
        if (fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            String contents = rd.getDocument().toStringUtf8();
            return contents;
        }
        return null;
    }

    public DBObject getDocumentAsBson() {
        if (fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            DBObject dbObject = BsonHelper.dbObjectFromResultDocument(rd);
            return dbObject;
        }
        return null;
    }

    public <T> T getDocument(Mapper<T> mapper) throws Exception {
        if (fetchResponse.hasResultDocument()) {
            return mapper.fromResultDocument(fetchResponse.getResultDocument());
        }
        return null;
    }

    public AssociatedDocument getAssociatedDocument(int index) {
        return fetchResponse.getAssociatedDocumentList().get(index);
    }

    public List<AssociatedDocument> getAssociatedDocuments() {
        return fetchResponse.getAssociatedDocumentList();
    }

    public boolean hasResultDocument() {
        return fetchResponse.hasResultDocument();
    }

    public int getAssociatedDocumentCount() {
        return fetchResponse.getAssociatedDocumentCount();
    }


}
