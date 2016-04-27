package org.lumongo.doc;

import com.google.protobuf.ByteString;
import org.bson.BSON;
import org.bson.BsonBinaryReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.lumongo.cluster.message.Lumongo;

import java.nio.ByteBuffer;

/**
 * Created by Matt Davis on 2/1/16.
 */
public class ResultHelper {

	public static Document getDBObjectFromScoredResult(Lumongo.ScoredResult scoredResult) {
		if (scoredResult.hasResultDocument()) {
			Lumongo.ResultDocument rd = scoredResult.getResultDocument();
			ByteString bs = rd.getDocument();
			BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(bs.toByteArray()));
			return new DocumentCodec().decode(bsonReader, DecoderContext.builder().build());
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
