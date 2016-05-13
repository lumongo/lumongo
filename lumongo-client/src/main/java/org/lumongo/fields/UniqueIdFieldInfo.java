package org.lumongo.fields;

import org.bson.Document;

import java.lang.reflect.Field;

public class UniqueIdFieldInfo<T> {
	private final String fieldName;
	private final Field field;

	public UniqueIdFieldInfo(Field field, String fieldName) {
		this.fieldName = fieldName;
		this.field = field;
	}

	public String getFieldName() {
		return fieldName;
	}

	public String build(T object) throws IllegalArgumentException, IllegalAccessException {
		if (object != null) {
			Object o = field.get(object);
			if (o instanceof String) {
				return (String) o;
			}

		}
		throw new RuntimeException("Unique id field <" + field.getName() + "> must not be null");
	}

	public void populate(T newInstance, Document savedDocument) throws Exception {
		Object value = savedDocument.get("_id");
		field.set(newInstance, value);
	}

}
