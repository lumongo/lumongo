package org.lumongo.server.index.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LegacyIntField;

public class IntFieldIndexer extends NumericFieldIndexer {
	
	public static final IntFieldIndexer INSTANCE = new IntFieldIndexer();
	
	protected IntFieldIndexer() {
		
	}
	
	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new LegacyIntField(indexedFieldName, o.intValue(), Store.YES);
	}
	
}
