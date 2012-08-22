package org.lumongo.fields;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FactedFieldInfo<T> {
	private String facetPrefix;
	private Field field;

	public FactedFieldInfo(Field field, String facetPrefix) {
		this.facetPrefix = facetPrefix;
		this.field = field;
	}

	public String getFacetPrefix() {
		return facetPrefix;
	}

	public List<String> build(T object) throws IllegalArgumentException, IllegalAccessException {
		if (object != null) {
			ArrayList<String> list = new ArrayList<String>();
			Object o = field.get(object);

			if (o instanceof Collection<?>) {
				Collection<?> l = (Collection<?>) o;
				for (Object s : l) {
					list.add(s.toString());
				}
			}
			else if (o.getClass().isArray()) {
				Object[] l = (Object[]) o;
				for (Object s : l) {
					list.add(s.toString());
				}
			}
			else {
				list.add(o.toString());
			}

			return list;
		}

		return Collections.emptyList();

	}


}
