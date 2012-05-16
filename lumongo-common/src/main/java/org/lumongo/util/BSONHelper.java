package org.lumongo.util;

import java.util.Collection;

import org.bson.BSON;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class BSONHelper {
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
		return dbObjectToResultDocument(uniqueId, document, null);
	}
	
	public static ResultDocument dbObjectToResultDocument(String uniqueId, DBObject document, Collection<Metadata> metadata) {
		
		ByteString byteString = ByteString.copyFrom(BSON.encode(document));
		ResultDocument.Builder builder = ResultDocument.newBuilder();
		builder.setDocument(byteString);
		if (metadata != null) {
			builder.addAllMetadata(metadata);
		}
		builder.setType(ResultDocument.Type.BSON);
		builder.setUniqueId(uniqueId);
		return builder.build();
	}
}
