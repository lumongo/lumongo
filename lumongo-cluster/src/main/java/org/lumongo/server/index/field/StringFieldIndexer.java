package org.lumongo.server.index.field;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;

public class StringFieldIndexer extends FieldIndexer {
	
	private static FieldType notStoredTextField;
	
	static {
		notStoredTextField = new FieldType(TextField.TYPE_NOT_STORED);
		notStoredTextField.freeze();
	}
	
	public static final StringFieldIndexer INSTANCE = new StringFieldIndexer();
	
	protected StringFieldIndexer() {
		
	}
	
	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value != null) {
			d.add((new Field(indexedFieldName, value.toString(), notStoredTextField)));
		}
	}
	
}
