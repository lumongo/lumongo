package org.lumongo.fields;

import org.bson.Document;
import org.lumongo.util.AnnotationUtil;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SavedEmbeddedFieldInfo<T> {
	private final String fieldName;
	private final Field field;
	private final SavedFieldsMapper savedFieldMapper;
	private boolean fieldIsList;

	public SavedEmbeddedFieldInfo(Field field, String fieldName) {
		this.fieldName = fieldName;
		this.field = field;

		Class<?> type = field.getType();
		this.fieldIsList = List.class.isAssignableFrom(type);

		if (fieldIsList) {
			Type genericType = field.getGenericType();
			if (genericType instanceof ParameterizedType) {
				ParameterizedType pType = (ParameterizedType) genericType;
				type = (Class<?>) pType.getActualTypeArguments()[0];
			}
		}

		this.savedFieldMapper = new SavedFieldsMapper(type);

		List<Field> allFields = AnnotationUtil.getNonStaticFields(type, true);

		for (Field f : allFields) {
			f.setAccessible(true);
			savedFieldMapper.setupField(f);
		}
	}

	public String getFieldName() {
		return fieldName;
	}

	public Object getValue(T object) throws Exception {

		if (fieldIsList) {

			Object o = field.get(object);
			List<?> l = (List<?>) o;

			List<Document> retValues = new ArrayList<>();
			for (Object o2 : l) {
				Document retVal = savedFieldMapper.toDocument(o2);
				retValues.add(retVal);
			}
			return retValues;
		}
		else {
			Object o = field.get(object);
			Document returnValue = savedFieldMapper.toDocument(o);
			return returnValue;
		}
	}

	public void populate(T newInstance, Document document) throws Exception {

		Object value = document.get(fieldName);

		boolean valuesIsList = value instanceof List;

		if (valuesIsList) {
			List<Document> embeddedValues = (List<Document>) value;
			if (fieldIsList) {

				List<Object> objs = new ArrayList<>();
				for (Document embeddedValue : embeddedValues) {
					objs.add(savedFieldMapper.fromDBObject(embeddedValue));
				}
				field.set(newInstance, objs);
			}
			else {
				List<?> valueList = (List<?>) value;
				if (valueList.size() == 1) {
					Object first = valueList.iterator().next();
					if (first != null) {
						field.set(newInstance, savedFieldMapper.fromDBObject((Document) first));
					}
				}
				else if (valueList.isEmpty()) {

				}
				else {
					throw new Exception("Cannot assign multiple values <" + valueList + "> to field <" + field.getName() + "> with type <" + field.getType()
									+ "> because it is not a list.");
				}
			}
		}
		else {
			Object obj = savedFieldMapper.fromDBObject((Document) value);
			if (fieldIsList) {
				if (value != null) {
					field.set(newInstance, new ArrayList<>(Arrays.asList(obj)));
				}
			}
			else {
				field.set(newInstance, obj);
			}
		}

	}
}
