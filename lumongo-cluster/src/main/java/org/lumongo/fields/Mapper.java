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

	public void Mapper(Class<?> clazz) {
		this.clazz = clazz;

		HashSet<Field> allFields = AnnotationUtil.getNonStaticFields(clazz, true);

		for (Field f : allFields) {
			String fieldName = f.getName();
			boolean saved = false;
			boolean faceted = false;
			boolean indexed = false;
			LMAnalyzer lma = null;

			if (f.isAnnotationPresent(Indexed.class)) {
				indexed = true;
				Indexed in = f.getAnnotation(Indexed.class);
				lma = in.value();
			}
			if (f.isAnnotationPresent(Saved.class)) {
				saved = true;
			}
			if (f.isAnnotationPresent(Faceted.class)) {
				faceted = true;
			}
			if (f.isAnnotationPresent(AsField.class)) {
				AsField as = f.getAnnotation(AsField.class);
				fieldName = as.value();
			}
		}
	}


}
