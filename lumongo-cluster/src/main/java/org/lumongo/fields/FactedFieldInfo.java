package org.lumongo.fields;

import java.lang.reflect.Field;

public class FactedFieldInfo {
	private String facetPrefix;
	private Field field;
	
	public FactedFieldInfo(Field field, String facetPrefix) {
		this.facetPrefix = facetPrefix;
		this.field = field;
	}

	public String getFacetPrefix() {
		return facetPrefix;
	}


}
