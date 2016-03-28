package org.lumongo.server.index.field;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;

public class StringFieldIndexer extends FieldIndexer {
	
	private static FieldType notStoredTextField;
	
	static {
		notStoredTextField = new FieldType(TextField.TYPE_NOT_STORED);

		// For PostingsHighlighter in Lucene 4.1 +
		// notStoredTextField.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		// example: https://svn.apache.org/repos/asf/lucene/dev/trunk/lucene/highlighter/src/test/org/apache/lucene/search/postingshighlight/TestPostingsHighlighter.java
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
