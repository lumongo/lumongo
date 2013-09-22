package org.lumongo.doc;

import java.util.Date;

import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.util.StringUtil;

public class IndexedDocBuilder {
	
	private LMDoc.Builder indexedDocBuilder;
	
	public static IndexedDocBuilder newBuilder() {
		return new IndexedDocBuilder();
	}
	
	public IndexedDocBuilder() {
		indexedDocBuilder = LMDoc.newBuilder();
	}
	
	public IndexedDocBuilder addField(String fieldName, String... values) {
		
		for (String value : values) {
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addFieldValue(value));
		}
		
		return this;
	}
	
	public IndexedDocBuilder addField(String fieldName, Integer... values) {
		
		for (Integer value : values) {
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addIntValue(value));
		}
		
		return this;
	}
	
	public IndexedDocBuilder addField(String fieldName, Long... values) {
		
		for (Long value : values) {
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addLongValue(value));
		}
		
		return this;
	}
	
	public IndexedDocBuilder addField(String fieldName, Float... values) {
		
		for (Float value : values) {
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addFloatValue(value));
		}
		return this;
	}
	
	public IndexedDocBuilder addField(String fieldName, Double... values) {
		
		for (Double value : values) {
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addDoubleValue(value));
		}
		return this;
	}
	
	public IndexedDocBuilder addField(String fieldName, Date... values) {
		
		for (Date value : values) {
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addLongValue(value.getTime()));
		}
		return this;
	}
	
	public void addFacet(String... path) {
		indexedDocBuilder.addFacet(StringUtil.join(LumongoConstants.FACET_DELIMITER, path));
	}
	
	public IndexedDocBuilder clearFields() {
		indexedDocBuilder.clearIndexedField();
		return this;
	}
	
	public IndexedDocBuilder clearFacets() {
		indexedDocBuilder.clearFacet();
		return this;
	}
	
	public IndexedDocBuilder clear() {
		indexedDocBuilder.clear();
		return this;
	}
	
	public LMDoc getIndexedDoc() {
		return indexedDocBuilder.build();
	}
	
}
