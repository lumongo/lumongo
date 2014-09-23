package org.lumongo.server.indexing.field;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;

public class DoubleFieldIndexer extends NumericFieldIndexer {
	
	public static final DoubleFieldIndexer INSTANCE = new DoubleFieldIndexer();
	
	protected DoubleFieldIndexer() {
		
	}
	
	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new DoubleField(indexedFieldName, o.doubleValue(), Store.YES);
	}
	
}
