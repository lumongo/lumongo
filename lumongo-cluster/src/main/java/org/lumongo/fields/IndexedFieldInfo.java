package org.lumongo.fields;

import java.lang.reflect.Field;

import org.lumongo.cluster.message.Lumongo.LMAnalyzer;

public class IndexedFieldInfo {
	private String fieldName;
	private Field field;
	private LMAnalyzer lmAnalyzer;

	public IndexedFieldInfo(Field field, String fieldName, LMAnalyzer lmAnalyzer) {
		this.fieldName = fieldName;
		this.field = field;
		this.lmAnalyzer = lmAnalyzer;
	}

	public String getFieldName() {
		return fieldName;
	}
}
