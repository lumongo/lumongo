package org.lumongo.util;

import org.bson.Document;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.util.LumongoUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Matt Davis on 2/1/16.
 */
public class ResultHelper {

	public static Document getDocumentFromScoredResult(Lumongo.ScoredResult scoredResult) {
		if (scoredResult.hasResultDocument()) {
			Lumongo.ResultDocument rd = scoredResult.getResultDocument();
			return getDocumentFromResultDocument(rd);
		}
		return null;
	}

	public static Document getDocumentFromResultDocument(Lumongo.ResultDocument rd) {
		if (rd.hasDocument()) {
			return LumongoUtil.byteArrayToMongoDocument(rd.getDocument().toByteArray());
		}
		return null;
	}

	public static Object getValueFromMongoDocument(org.bson.Document mongoDocument, String storedFieldName) {

		Object o;
		if (storedFieldName.contains(".")) {
			o = mongoDocument;
			String[] fields = storedFieldName.split("\\.");
			for (String field : fields) {
				if (o instanceof List) {
					List<?> list = (List<?>) o;
					List<Object> values = new ArrayList<>();
					list.stream().filter(item -> item instanceof org.bson.Document).forEach(item -> {
						org.bson.Document dbObj = (org.bson.Document) item;
						Object object = dbObj.get(field);
						if (object != null) {
							values.add(object);
						}
					});
					if (!values.isEmpty()) {
						o = values;
					}
					else {
						o = null;
					}
				}
				else if (o instanceof org.bson.Document) {
					org.bson.Document mongoDoc = (org.bson.Document) o;
					o = mongoDoc.get(field);
				}
				else {
					o = null;
					break;
				}
			}
		}
		else {
			o = mongoDocument.get(storedFieldName);
		}

		return o;
	}

}
