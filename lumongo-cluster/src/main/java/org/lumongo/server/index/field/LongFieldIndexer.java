package org.lumongo.server.index.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LegacyLongField;

public class LongFieldIndexer extends NumericFieldIndexer {

	public static final LongFieldIndexer INSTANCE = new LongFieldIndexer();

	protected LongFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new LegacyLongField(indexedFieldName, o.longValue(), Store.YES);
	}

}
