package org.lumongo.fields;

import org.bson.Document;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Embedded;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.IndexedFields;
import org.lumongo.fields.annotations.NotSaved;
import org.lumongo.fields.annotations.UniqueId;

import java.lang.reflect.Field;
import java.util.HashSet;

public class SavedFieldsMapper<T> {

	private final Class<T> clazz;

	private HashSet<SavedFieldInfo<T>> savedFields;

	private HashSet<SavedEmbeddedFieldInfo<T>> savedEmbeddedFields;

	public SavedFieldsMapper(Class<T> clazz) {
		this.clazz = clazz;
		this.savedFields = new HashSet<>();
		this.savedEmbeddedFields = new HashSet<>();

	}

	public void setupField(Field f) {

		validate(f);

		String fieldName = f.getName();

		if (f.isAnnotationPresent(AsField.class)) {
			AsField as = f.getAnnotation(AsField.class);
			fieldName = as.value();
		}

		if (f.isAnnotationPresent(Embedded.class)) {
			savedEmbeddedFields.add(new SavedEmbeddedFieldInfo<>(f, fieldName));
		}
		else if (f.isAnnotationPresent(NotSaved.class)) {

		}
		else {
			savedFields.add(new SavedFieldInfo<>(f, fieldName));
		}

	}

	protected void validate(Field f) {
		if (f.isAnnotationPresent(NotSaved.class)) {
			if (f.isAnnotationPresent(IndexedFields.class) || f.isAnnotationPresent(Indexed.class) || f.isAnnotationPresent(Faceted.class) || f
							.isAnnotationPresent(UniqueId.class) || f.isAnnotationPresent(DefaultSearch.class) || f.isAnnotationPresent(Embedded.class)) {
				throw new RuntimeException(
								"Cannot use NotSaved with Indexed, Faceted, UniqueId, DefaultSearch, or Embedded on field <" + f.getName() + "> for class <"
												+ clazz.getSimpleName() + ">");
			}

		}
	}

	protected Document toDocument(T object) throws Exception {
		Document document = new Document();
		for (SavedFieldInfo<T> sfi : savedFields) {
			Object o = sfi.getValue(object);
			document.put(sfi.getFieldName(), o);
		}

		for (SavedEmbeddedFieldInfo<T> sefi : savedEmbeddedFields) {
			Object o = sefi.getValue(object);
			document.put(sefi.getFieldName(), o);
		}
		return document;
	}

	protected T fromDBObject(Document savedDocument) throws Exception {
		T newInstance = clazz.newInstance();
		for (SavedFieldInfo<T> sfi : savedFields) {
			sfi.populate(newInstance, savedDocument);
		}
		for (SavedEmbeddedFieldInfo<T> sefi : savedEmbeddedFields) {
			sefi.populate(newInstance, savedDocument);
		}

		return newInstance;
	}
}
