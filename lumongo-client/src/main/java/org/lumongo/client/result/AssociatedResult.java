package org.lumongo.client.result;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSON;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.Metadata;

import java.util.HashMap;
import java.util.Map;

public class AssociatedResult {

	private AssociatedDocument associatedDocument;

	public AssociatedResult(AssociatedDocument associatedDocument) {
		this.associatedDocument = associatedDocument;
	}

	public Map<String, String> getMeta() {
		HashMap<String, String> metadata = new HashMap<String, String>();
		for (Metadata md : associatedDocument.getMetadataList()) {
			metadata.put(md.getKey(), md.getValue());
		}
		return metadata;
	}

	public String getFilename() {
		return associatedDocument.getFilename();
	}

	public String getDocumentUniqueId() {
		return associatedDocument.getDocumentUniqueId();
	}

	public byte[] getDocumentAsBytes() {
		if (hasDocument()) {
			return associatedDocument.getDocument().toByteArray();
		}
		return null;
	}

	public String getDocumentAsUtf8() {
		if (hasDocument()) {
			String contents = associatedDocument.getDocument().toStringUtf8();
			return contents;
		}
		return null;
	}

	public DBObject getDocumentAsBson() {
		if (hasDocument()) {
			ByteString bs = associatedDocument.getDocument();
			DBObject document = new BasicDBObject();
			document.putAll(BSON.decode(bs.toByteArray()));
			return document;
		}
		return null;
	}

	public boolean hasDocument() {
		return associatedDocument.hasDocument();
	}

	public boolean getCompressed() {
		return associatedDocument.getCompressed();
	}

	public long getTimestamp() {
		return associatedDocument.getTimestamp();
	}

}
