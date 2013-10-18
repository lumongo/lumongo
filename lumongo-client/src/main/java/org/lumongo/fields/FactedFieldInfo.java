package org.lumongo.fields;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.lumongo.LumongoConstants;

public class FactedFieldInfo<T> {
	private final String facetPrefix;
	private final Field field;
	
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
			
			if (o != null) {
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
				else if (o instanceof Date) {
					Date d = (Date) o;
					Calendar cal = Calendar.getInstance();
					cal.setTime(d);
					//TODO configurable
					int year = cal.get(Calendar.YEAR);
					int month = cal.get(Calendar.MONTH) + 1;
					int day = cal.get(Calendar.DAY_OF_MONTH) + 1;
					list.add(LumongoConstants.FACET_JOINER.join(year, month, day));
				}
				else {
					list.add(o.toString());
				}
				
				return list;
			}
		}
		
		return Collections.emptyList();
		
	}
	
}
