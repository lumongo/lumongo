package org.lumongo.client.result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.fields.Mapper;
import org.lumongo.util.ResultDocHelper;

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
        if (fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            return rd.getDocument().toByteArray();
        }
        return null;
    }

    public Map<String, String> getMeta() {
        if (fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            HashMap<String, String> metadata = new HashMap<String, String>();
            for (Metadata md : rd.getMetadataList()) {
                metadata.put(md.getKey(), md.getValue());
            }
            return metadata;
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
            DBObject dbObject = ResultDocHelper.dbObjectFromResultDocument(rd);
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

    public Long getDocumentTimestamp() {
        if (fetchResponse.hasResultDocument()) {
            ResultDocument rd = fetchResponse.getResultDocument();
            return rd.getTimestamp();
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nFetchResult: {\n");
        if (hasResultDocument()) {
            sb.append("  ResultDocument: {\n");
            sb.append("    uniqueId: ");
            sb.append(getUniqueId());
            sb.append("\n");

            sb.append("    document: ");
            if (isDocumentBson()) {
                sb.append(getDocumentAsBson());
            }
            else if (isDocumentText()) {
                sb.append(getDocumentAsUtf8());
            }
            else if (isDocumentBinary()) {
                sb.append("BINARY");
            }
            sb.append("\n  }\n");

        }
        if (getAssociatedDocumentCount() != 0) {
            for (AssociatedDocument ad : getAssociatedDocuments()) {
                sb.append("  AssociatedDocument: {\n");
                sb.append("    filename: ");
                sb.append(ad.getFilename());
                sb.append("\n  }\n");
            }
        }
        sb.append("}\n");
        return sb.toString();
    }

}
