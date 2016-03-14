package org.lumongo.server.index.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LegacyDoubleField;

public class DoubleFieldIndexer extends NumericFieldIndexer {

	public static final DoubleFieldIndexer INSTANCE = new DoubleFieldIndexer();

	protected DoubleFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new LegacyDoubleField(indexedFieldName, o.doubleValue(), Store.YES);
	}

}
