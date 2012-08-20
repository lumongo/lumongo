package org.lumongo.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;

public class AnnotationUtil {
	public static HashSet<Field> getNonStaticFields(final Class<?> clazz, boolean deep) {
		HashSet<Field> fields = new HashSet<Field>();
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
