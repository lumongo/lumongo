package org.lumongo.fields;

import com.mongodb.DBObject;
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

			List<DBObject> retValues = new ArrayList<>();
			for (Object o2 : l) {
				DBObject retVal = savedFieldMapper.toDbObject(o2);
				retValues.add(retVal);
			}
			return retValues;
		}
		else {
			Object o = field.get(object);
			DBObject returnValue = savedFieldMapper.toDbObject(o);
			return returnValue;
		}
	}

	public void populate(T newInstance, DBObject savedDBObject) throws Exception {

		Object value = savedDBObject.get(fieldName);

		boolean valuesIsList = value instanceof List;

		if (valuesIsList) {
			List<DBObject> embeddedValues = (List<DBObject>) value;
			if (fieldIsList) {

				List<Object> objs = new ArrayList<>();
				for (DBObject embeddedValue : embeddedValues) {
					objs.add(savedFieldMapper.fromDBObject(embeddedValue));
				}
				field.set(newInstance, objs);
			}
			else {
				List<?> valueList = (List<?>) value;
				if (valueList.size() == 1) {
					Object first = valueList.iterator().next();
					if (first != null) {
						field.set(newInstance, savedFieldMapper.fromDBObject((DBObject) first));
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
			Object obj = savedFieldMapper.fromDBObject((DBObject) value);
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
