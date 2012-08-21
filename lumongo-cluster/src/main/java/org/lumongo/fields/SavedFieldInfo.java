package org.lumongo.fields;

import java.lang.reflect.Field;

public class SavedFieldInfo {
	private String fieldName;
	private Field field;

	public SavedFieldInfo(Field field, String fieldName) {
		this.fieldName = fieldName;
		this.field = field;
	}

	public String getFieldName() {
		return fieldName;
	}

}
