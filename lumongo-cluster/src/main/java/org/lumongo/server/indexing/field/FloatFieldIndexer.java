package org.lumongo.server.indexing.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatField;

public class FloatFieldIndexer extends NumericFieldIndexer {
	
	public static final FloatFieldIndexer INSTANCE = new FloatFieldIndexer();
	
	protected FloatFieldIndexer() {
		
	}
	
	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new FloatField(indexedFieldName, o.floatValue(), Store.YES);
	}
	
}
