package org.lumongo.fields;

import java.lang.reflect.Field;
import java.util.HashSet;

import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Saved;
import org.lumongo.util.AnnotationUtil;

public class Mapper {
	private HashSet<String> storedFields;

	private Class<?> clazz;

	private HashSet<FactedFieldInfo> facetedFields;
	private HashSet<IndexedFieldInfo> indexedFields;
	private HashSet<SavedFieldInfo> savedFields;
	
	public void Mapper(Class<?> clazz) {
		this.facetedFields = new HashSet<FactedFieldInfo>();
		this.indexedFields = new HashSet<IndexedFieldInfo>();
		this.savedFields = new HashSet<SavedFieldInfo>();
		
		this.clazz = clazz;

		HashSet<Field> allFields = AnnotationUtil.getNonStaticFields(clazz, true);

		for (Field f : allFields) {
			String fieldName = f.getName();

			LMAnalyzer lma = null;

			if (f.isAnnotationPresent(AsField.class)) {
				AsField as = f.getAnnotation(AsField.class);
				fieldName = as.value();
			}
			
			if (f.isAnnotationPresent(Indexed.class)) {
				Indexed in = f.getAnnotation(Indexed.class);
				lma = in.value();
				indexedFields.add(new IndexedFieldInfo(f, fieldName, lma));
			}
			if (f.isAnnotationPresent(Saved.class)) {
				@SuppressWarnings("unused")
				Saved saved = f.getAnnotation(Saved.class);
				savedFields.add(new SavedFieldInfo(f, fieldName));
			}
			if (f.isAnnotationPresent(Faceted.class)) {
				@SuppressWarnings("unused")
				Faceted faceted = f.getAnnotation(Faceted.class);
				facetedFields.add(new FactedFieldInfo(f, fieldName));
			}
			
		}
	}


}
