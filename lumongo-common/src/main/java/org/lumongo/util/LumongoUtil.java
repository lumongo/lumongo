package org.lumongo.util;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.lumongo.cluster.message.Lumongo;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Consumer;

public class LumongoUtil {

	public static void handleLists(Object o, Consumer<? super Object> action) {
		if (o instanceof Collection) {
			Collection<?> c = (Collection<?>) o;
			c.stream().filter((obj) -> obj != null).forEach(action);
		}
		else if (o instanceof Object[]) {
			Object[] arr = (Object[]) o;
			for (Object obj : arr) {
				if (obj != null) {
					action.accept(action);
				}
			}
		}
		else {
			if (o != null) {
				action.accept(o);
			}
		}
	}

	public static byte[] mongoDocumentToByteArray(Document mongoDocument) {
		BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
		BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
		new DocumentCodec().encode(writer, mongoDocument, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
		return outputBuffer.toByteArray();
	}

	public static Document resultDocumentToMongoDocument(Lumongo.ResultDocument resultDocument) {
		return byteArrayToMongoDocument(resultDocument.getDocument().toByteArray());
	}

	public static Document byteArrayToMongoDocument(byte[] byteArray) {
		BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(byteArray));
		return new DocumentCodec().decode(bsonReader, DecoderContext.builder().build());
	}
}
