package org.lumongo.server.indexing.field;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public abstract class NumericFieldIndexer extends FieldIndexer {
	
	protected NumericFieldIndexer() {
		
	}
	
	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value instanceof Number) {
			d.add(createField((Number) value, indexedFieldName));
		}
		else {
			throw new Exception("Expecting collection of Number or Number for field <" + storedFieldName + "> and found <" + value.getClass().getSimpleName()
							+ ">");
		}
	}
	
	protected abstract Field createField(Number o, String indexedFieldName);
	
}
