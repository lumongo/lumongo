package org.lumongo.server.index.field;

import org.apache.lucene.document.Document;
import org.lumongo.util.LumongoUtil;

public abstract class FieldIndexer {

	protected FieldIndexer() {

	}

	public void index(Document document, String storedFieldName, Object storedValue, String indexedFieldName) throws Exception {

		LumongoUtil.handleLists(storedValue, obj -> {
			try {
				handleValue(document, storedFieldName, obj, indexedFieldName);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

	}

	protected abstract void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception;

}
