package org.lumongo.util;

import java.util.Map;

import org.bson.BSON;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ResultDocHelper {
	public static DBObject dbObjectFromResultDocument(ResultDocument rd) {
		if (rd.hasDocument()) {
			if (rd.getType().equals(ResultDocument.Type.BSON)) {
				ByteString bs = rd.getDocument();
				DBObject document = new BasicDBObject();
				document.putAll(BSON.decode(bs.toByteArray()));
				return document;
			}
			else {
				throw new IllegalArgumentException("Raw document must be of type <" + ResultDocument.Type.BSON + ">");
			}
		}
		return null;
	}

	public static ResultDocument dbObjectToResultDocument(String uniqueId, DBObject document) {
		return dbObjectToResultDocument(uniqueId, document, false);
	}

	public static ResultDocument dbObjectToResultDocument(String uniqueId, DBObject document, Boolean compressed) {
		return dbObjectToResultDocument(uniqueId, document, compressed, null);
	}

	public static void addMetaData(Map<String, String> metadata, ResultDocument.Builder resultDocumentBuilder) {
		if (metadata != null) {
			for (String key : metadata.keySet()) {
				resultDocumentBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue(metadata.get(key)));
			}
		}
	}

	public static ResultDocument dbObjectToResultDocument(String uniqueId, DBObject document, Boolean compressed, Map<String, String> metadata) {

		ByteString byteString = ByteString.copyFrom(BSON.encode(document));
		ResultDocument.Builder builder = ResultDocument.newBuilder();
		builder.setDocument(byteString);
		if (metadata != null) {
			addMetaData(metadata, builder);
		}
		builder.setType(ResultDocument.Type.BSON);
		builder.setUniqueId(uniqueId);
		if (compressed != null) {
			builder.setCompressed(compressed);
		}
		return builder.build();
	}
}
