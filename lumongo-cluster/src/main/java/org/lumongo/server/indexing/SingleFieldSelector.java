package org.lumongo.server.indexing;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;

public class SingleFieldSelector implements FieldSelector {
	
	private static final long serialVersionUID = 1L;
	
	private String fieldName;
	
	public SingleFieldSelector(String fieldName) {
		this.fieldName = fieldName;
	}
	
	@Override
	public FieldSelectorResult accept(String fieldName) {
		return (this.fieldName.equals(fieldName) ? FieldSelectorResult.LOAD : FieldSelectorResult.NO_LOAD);
	}
	
}
