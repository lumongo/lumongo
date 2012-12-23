package org.lumongo.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.cluster.message.Lumongo.ResultDocument;

import com.mongodb.BasicDBObject;

public class BsonDocumentBuilder {
	private HashSet<String> fieldsToStore;
	
	public BsonDocumentBuilder() {
		fieldsToStore = new HashSet<String>();
	}
	
	public void addFieldsToStore(String... fields) {
		for (String field : fields) {
			fieldsToStore.add(field);
		}
	}
	
	public ResultDocument getResultDocument(String uniqueId, LMDoc lmDoc) {
		
		HashMap<String, List<Object>> fieldToValueMap = new HashMap<String, List<Object>>();
		for (LMField lmField : lmDoc.getIndexedFieldList()) {
			String fieldName = lmField.getFieldName();
			if (fieldsToStore.contains(fieldName)) {
				if (!fieldToValueMap.containsKey(fieldName)) {
					fieldToValueMap.put(fieldName, new ArrayList<Object>());
				}
				for (String fieldValue : lmField.getFieldValueList()) {
					fieldToValueMap.get(fieldName).add(fieldValue);
				}
				for (Double fieldValue : lmField.getDoubleValueList()) {
					fieldToValueMap.get(fieldName).add(fieldValue);
				}
				for (Float fieldValue : lmField.getFloatValueList()) {
					fieldToValueMap.get(fieldName).add(fieldValue);
				}
				for (Integer fieldValue : lmField.getIntValueList()) {
					fieldToValueMap.get(fieldName).add(fieldValue);
				}
				for (Long fieldValue : lmField.getLongValueList()) {
					fieldToValueMap.get(fieldName).add(fieldValue);
				}
			}
		}
		
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.putAll(fieldToValueMap);
		
		ResultDocument rd = ResultDocHelper.dbObjectToResultDocument(uniqueId, dbObject);
		
		return rd;
	}
	
}
