package org.lumongo.server.indexing.field;

import java.util.Collection;

import org.apache.lucene.document.Document;

public abstract class FieldIndexer {
	
	protected FieldIndexer() {
		
	}
	
	public void index(Document document, String storedFieldName, Object storedValue, String indexedFieldName) throws Exception {
		
		if (storedValue instanceof Collection) {
			Collection<?> collection = (Collection<?>) storedValue;
			for (Object co : collection) {
				handleValue(document, storedFieldName, co, indexedFieldName);
			}
		}
		else {
			handleValue(document, storedFieldName, storedValue, indexedFieldName);
		}
		
	}
	
	protected abstract void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception;
	
}
