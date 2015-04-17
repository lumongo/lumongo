package org.lumongo.fields;

import org.lumongo.cluster.message.Lumongo.LMFacet;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

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

	public List<LMFacet> build(T object) throws IllegalArgumentException, IllegalAccessException {
		if (object != null) {
			ArrayList<LMFacet> list = new ArrayList<>();
			Object o = field.get(object);

			if (o != null) {

				if (o instanceof Collection<?>) {
					Collection<?> l = (Collection<?>) o;
					for (Object s : l) {
						LMFacet.Builder lmFacetBuilder = LMFacet.newBuilder().setLabel(facetPrefix);
						lmFacetBuilder.addPath(s.toString());
						list.add(lmFacetBuilder.build());
					}
				}
				else if (o.getClass().isArray()) {
					Object[] l = (Object[]) o;
					for (Object s : l) {
						LMFacet.Builder lmFacetBuilder = LMFacet.newBuilder().setLabel(facetPrefix);
						lmFacetBuilder.addPath(s.toString());
						list.add(lmFacetBuilder.build());
					}
				}
				else if (o instanceof Date) {
					Date d = (Date) o;
					Calendar cal = Calendar.getInstance();
					cal.setTime(d);

					//TODO configurable
					int year = cal.get(Calendar.YEAR);
					int month = cal.get(Calendar.MONTH) + 1;
					int day = cal.get(Calendar.DAY_OF_MONTH);

					LMFacet.Builder lmFacetBuilder = LMFacet.newBuilder().setLabel(facetPrefix);
					lmFacetBuilder.addPath(year + "");
					lmFacetBuilder.addPath(month + "");
					lmFacetBuilder.addPath(day + "");
					list.add(lmFacetBuilder.build());
				}
				else {
					LMFacet.Builder lmFacetBuilder = LMFacet.newBuilder().setLabel(facetPrefix);
					lmFacetBuilder.addPath(o.toString());
					list.add(lmFacetBuilder.build());
				}

				return list;
			}
		}

		return Collections.emptyList();

	}

}
