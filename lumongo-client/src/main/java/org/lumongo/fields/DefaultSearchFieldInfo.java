package org.lumongo.fields;

import java.lang.reflect.Field;

public class DefaultSearchFieldInfo<T> {
	private final String fieldName;
	private final Field field;

	public DefaultSearchFieldInfo(Field field, String fieldName) {
		this.fieldName = fieldName;
		this.field = field;
	}

	public String getFieldName() {
		return fieldName;
	}

	public Field getField() {
		return field;
	}

}
