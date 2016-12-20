package org.lumongo.server.index.field;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public abstract class NumericFieldIndexer extends FieldIndexer {

	protected NumericFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value != null) {
			if (value instanceof Number) {
				d.add(createField((Number) value, indexedFieldName));
			}
			else if (value instanceof String) {
				try {
					d.add(createField(parseString((String) value), indexedFieldName));
				}
				catch (NumberFormatException e) {
					throw new Exception("String value <" + value + "> for field <" + storedFieldName + "> cannot be parsed as a the defined numeric type");
				}
			}
			else {
				throw new Exception("Expecting collection of Number, collection of numeric String, Number, or numeric String for field <" + storedFieldName
						+ "> and found <" + value.getClass().getSimpleName() + ">");
			}
		}
	}

	protected abstract Field createField(Number o, String indexedFieldName);

	protected abstract Number parseString(String value);

}
