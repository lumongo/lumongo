package org.lumongo.server.index.field;

import org.apache.lucene.document.Document;

import java.util.Collection;

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
		else if (storedValue instanceof Object[]) {
			Object[] arr = (Object[]) storedValue;
			for (Object co : arr) {
				handleValue(document, storedFieldName, co, indexedFieldName);
			}
		}
		else {
			handleValue(document, storedFieldName, storedValue, indexedFieldName);
		}
		
	}

	protected abstract void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception;
	
}
