package org.lumongo.server.index.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;

public class IntFieldIndexer extends NumericFieldIndexer {

	public static final IntFieldIndexer INSTANCE = new IntFieldIndexer();

	protected IntFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new IntPoint(indexedFieldName, o.intValue());
	}

	@Override
	protected Number parseString(String value) {
		return Integer.parseInt(value);
	}

}
