package org.lumongo.doc;

import com.google.protobuf.ByteString;
import org.bson.Document;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.util.LumongoUtil;

/**
 * Created by Matt Davis on 2/1/16.
 */
public class ResultHelper {

	public static Document getDBObjectFromScoredResult(Lumongo.ScoredResult scoredResult) {
		if (scoredResult.hasResultDocument()) {
			Lumongo.ResultDocument rd = scoredResult.getResultDocument();
			return LumongoUtil.byteArrayToMongoDocument(rd.getDocument().toByteArray());
		}
		return null;
	}

	public static Document getDocumentFromScoredResult(Lumongo.ScoredResult scoredResult) {
		if (scoredResult.hasResultDocument()) {
			Lumongo.ResultDocument rd = scoredResult.getResultDocument();
			ByteString bs = rd.getDocument();
			Document document = new Document();
			document.putAll(LumongoUtil.byteArrayToMongoDocument(bs.toByteArray()));
			return document;
		}
		return null;
	}

}
