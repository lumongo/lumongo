package org.lumongo.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class AnnotationUtil {
	public static List<Field> getNonStaticFields(final Class<?> clazz, boolean deep) {
		List<Field> fields = new ArrayList<>();
		for (Field f : clazz.getDeclaredFields()) {
			if (!Modifier.isStatic(f.getModifiers())) {
				fields.add(f);
			}
		}
		
		if (deep) {
			Class<?> parent = clazz.getSuperclass();
			while ((parent != null) && (parent != Object.class)) {
				for (Field f : parent.getDeclaredFields()) {
					if (!Modifier.isStatic(f.getModifiers())) {
						fields.add(f);
					}
				}
				
				parent = parent.getSuperclass();
			}
		}
		
		return fields;
	}
}
