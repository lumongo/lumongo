package org.lumongo.fields;

import java.lang.reflect.Field;

public class SavedFieldInfo<T> {
	private final String fieldName;
	private final Field field;

	public SavedFieldInfo(Field field, String fieldName) {
		this.fieldName = fieldName;
		this.field = field;
	}

	public String getFieldName() {
		return fieldName;
	}

	Object getValue(T object) throws IllegalArgumentException, IllegalAccessException {
		return field.get(object);
	}

}
