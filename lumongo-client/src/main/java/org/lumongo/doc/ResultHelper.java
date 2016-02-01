package org.lumongo.doc;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSON;
import org.bson.Document;
import org.lumongo.cluster.message.Lumongo;

/**
 * Created by Matt Davis on 2/1/16.
 */
public class ResultHelper {

	public static DBObject getDBObjectFromScoredResult(Lumongo.ScoredResult scoredResult) {
		if (scoredResult.hasResultDocument()) {
			Lumongo.ResultDocument rd = scoredResult.getResultDocument();
			ByteString bs = rd.getDocument();
			DBObject document = new BasicDBObject();
			document.putAll(BSON.decode(bs.toByteArray()));
			return document;
		}
		return null;
	}

	public static Document getDocumentFromScoredResult(Lumongo.ScoredResult scoredResult) {
		if (scoredResult.hasResultDocument()) {
			Lumongo.ResultDocument rd = scoredResult.getResultDocument();
			ByteString bs = rd.getDocument();
			Document document = new Document();
			document.putAll(BSON.decode(bs.toByteArray()).toMap());
			return document;
		}
		return null;
	}

}
