package org.lumongo.doc;

import org.bson.BSON;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.Metadata;

import com.google.protobuf.ByteString;
import com.mongodb.DBObject;

public class AssociatedBuilder {

    private AssociatedDocument.Builder adBuilder;

    public AssociatedBuilder(String uniqueId, String filename) {
        adBuilder = AssociatedDocument.newBuilder();
        adBuilder.setFilename(filename);
        adBuilder.setDocumentUniqueId(uniqueId);
    }

    public AssociatedBuilder setCompressed(boolean compressed) {
        adBuilder.setCompressed(compressed);
        return this;
    }

    public AssociatedBuilder setDocument(String utf8Text) {
        adBuilder.setDocument(ByteString.copyFromUtf8(utf8Text));
        return this;
    }

    public AssociatedBuilder setDocument(byte[] bytes) {
        adBuilder.setDocument(ByteString.copyFrom(bytes));
        return this;
    }

    public AssociatedBuilder setDocument(DBObject document) {
        adBuilder.setDocument(ByteString.copyFrom(BSON.encode(document)));
        return this;
    }

    public AssociatedBuilder addMetaData(String key, String value) {
        adBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue(value));
        return this;
    }

    public AssociatedDocument getAssociatedDocument() {
        return adBuilder.build();
    }

}
