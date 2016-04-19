package org.lumongo.server.index.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;

public class FloatFieldIndexer extends NumericFieldIndexer {

	public static final FloatFieldIndexer INSTANCE = new FloatFieldIndexer();

	protected FloatFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new FloatPoint(indexedFieldName, o.floatValue());
	}

	@Override
	protected Number parseString(String value) {
		return Float.parseFloat(value);
	}

}
