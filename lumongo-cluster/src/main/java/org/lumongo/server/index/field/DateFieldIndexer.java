package org.lumongo.server.index.field;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;

import java.util.Date;

public class DateFieldIndexer extends FieldIndexer {
	
	public static final DateFieldIndexer INSTANCE = new DateFieldIndexer();
	
	protected DateFieldIndexer() {
		
	}
	
	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value instanceof Date) {
			d.add(createField((Date) value, indexedFieldName));
		}
		else {
			throw new Exception(
							"Expecting collection of Date or Date for field <" + storedFieldName + "> and found <" + value.getClass().getSimpleName() + ">");
		}
	}
	
	protected Field createField(Date o, String indexedFieldName) {
		return new LongField(indexedFieldName, o.getTime(), Store.YES);
	}
	
}
