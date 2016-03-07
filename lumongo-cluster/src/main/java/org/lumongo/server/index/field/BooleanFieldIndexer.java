package org.lumongo.server.index.field;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;

public class BooleanFieldIndexer extends FieldIndexer {

	public static final BooleanFieldIndexer INSTANCE = new BooleanFieldIndexer();
	private static FieldType notStoredTextField;

	static {
		notStoredTextField = new FieldType(TextField.TYPE_NOT_STORED);
		notStoredTextField.setStoreTermVectors(true);
		notStoredTextField.setStoreTermVectorOffsets(true);
		notStoredTextField.setStoreTermVectorPositions(true);
		// For PostingsHighlighter in Lucene 4.1 +
		// notStoredTextField.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		// example: https://svn.apache.org/repos/asf/lucene/dev/trunk/lucene/highlighter/src/test/org/apache/lucene/search/postingshighlight/TestPostingsHighlighter.java
		notStoredTextField.freeze();
	}

	protected BooleanFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value != null) {
			if (value instanceof Boolean) {
				d.add((new Field(indexedFieldName, value.toString(), notStoredTextField)));
			}
			else if (value instanceof String) {
				String v = (String)value;
				if (v.startsWith("T") || v.startsWith("F") | v.startsWith("t") | v.startsWith("f") || v.startsWith("0") || v.startsWith("1")) {
					d.add((new Field(indexedFieldName, v, notStoredTextField)));
				}
				else {
					throw new Exception(
							"String for Boolean field must start with 'T','t','F','f','0', or '1' for <" + storedFieldName + "> and found <" + v + ">");
				}
			}
			else if (value instanceof Number) {
				Number number = (Number)value;
				int v = number.intValue();
				if (v == 0 || v == 1) {
					d.add((new Field(indexedFieldName, String.valueOf(v), notStoredTextField)));
				}
				else {
					throw new Exception(
							"Number for Boolean field must be 0 or 1 for <" + storedFieldName + "> and found <" + v + ">");
				}
			}
			else {
				throw new Exception(
						"Expecting collection of data type of Boolean, String, or Number for field <" + storedFieldName + "> and found <" + value.getClass()
								.getSimpleName() + ">");

			}
		}
	}

}
