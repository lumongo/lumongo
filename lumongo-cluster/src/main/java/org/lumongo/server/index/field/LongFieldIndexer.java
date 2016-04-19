package org.lumongo.server.index.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;

public class LongFieldIndexer extends NumericFieldIndexer {

	public static final LongFieldIndexer INSTANCE = new LongFieldIndexer();

	protected LongFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new LongPoint(indexedFieldName, o.longValue());
	}

	@Override
	protected Number parseString(String value) {
		return Long.parseLong(value);
	}

}
