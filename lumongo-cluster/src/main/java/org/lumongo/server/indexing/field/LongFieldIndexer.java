package org.lumongo.server.indexing.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;

public class LongFieldIndexer extends NumericFieldIndexer {
	
	public static final LongFieldIndexer INSTANCE = new LongFieldIndexer();
	
	protected LongFieldIndexer() {
		
	}
	
	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new LongField(indexedFieldName, o.longValue(), Store.YES);
	}
	
}
