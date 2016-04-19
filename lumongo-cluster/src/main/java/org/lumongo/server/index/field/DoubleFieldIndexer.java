package org.lumongo.server.index.field;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;

public class DoubleFieldIndexer extends NumericFieldIndexer {

	public static final DoubleFieldIndexer INSTANCE = new DoubleFieldIndexer();

	protected DoubleFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new DoublePoint(indexedFieldName, o.doubleValue());
	}

	@Override
	protected Number parseString(String value) {
		return Double.parseDouble(value);
	}

}
