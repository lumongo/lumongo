package org.lumongo.fields;

import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.LumongoIndex;
import org.lumongo.cluster.message.LumongoIndex.FieldConfig;
import org.lumongo.cluster.message.LumongoIndex.FieldConfig.FieldType;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Embedded;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.FacetedFields;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.IndexedFields;
import org.lumongo.fields.annotations.Sorted;
import org.lumongo.fields.annotations.SortedFields;
import org.lumongo.fields.annotations.UniqueId;
import org.lumongo.util.AnnotationUtil;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.lumongo.cluster.message.LumongoIndex.*;
import static org.lumongo.cluster.message.LumongoIndex.IndexAs;

public class FieldConfigMapper<T> {

	private final String prefix;

	private final Class<T> clazz;

	private HashMap<String, FieldConfig> fieldConfigMap;

	private List<FieldConfigMapper<?>> embeddedFieldConfigMappers;

	public FieldConfigMapper(Class<T> clazz, String prefix) {
		this.clazz = clazz;
		this.prefix = prefix;
		this.fieldConfigMap = new HashMap<>();
		this.embeddedFieldConfigMappers = new ArrayList<>();
	}

	public void setupField(Field f) {
		String fieldName = f.getName();

		if (f.isAnnotationPresent(AsField.class)) {
			AsField as = f.getAnnotation(AsField.class);
			fieldName = as.value();
		}

		if (!prefix.isEmpty()) {
			fieldName = prefix + "." + fieldName;
		}

		Class<?> fieldType = f.getType();

		if (List.class.isAssignableFrom(fieldType)) {
			Type genericType = f.getGenericType();
			if (genericType instanceof ParameterizedType) {
				ParameterizedType pType = (ParameterizedType) genericType;
				fieldType = (Class<?>) pType.getActualTypeArguments()[0];
			}
		}

		if (f.isAnnotationPresent(Embedded.class)) {
			if (f.isAnnotationPresent(IndexedFields.class) || f.isAnnotationPresent(Indexed.class) || f.isAnnotationPresent(Faceted.class) || f
					.isAnnotationPresent(UniqueId.class) || f.isAnnotationPresent(DefaultSearch.class)) {
				throw new RuntimeException(
						"Cannot use Indexed, Faceted, UniqueId, DefaultSearch on embedded field <" + f.getName() + "> for class <" + clazz.getSimpleName()
								+ ">");
			}

			FieldConfigMapper fieldConfigMapper = new FieldConfigMapper<>(fieldType, fieldName);

			List<Field> allFields = AnnotationUtil.getNonStaticFields(fieldType, true);

			for (Field ef : allFields) {
				ef.setAccessible(true);
				fieldConfigMapper.setupField(ef);
			}
			embeddedFieldConfigMappers.add(fieldConfigMapper);
		}
		else {
			FieldConfig.Builder fieldConfigBuilder = FieldConfig.newBuilder();
			fieldConfigBuilder.setStoredFieldName(fieldName);

			if (fieldType.equals(String.class)) {
				fieldConfigBuilder.setFieldType(FieldType.STRING);
			}
			else if (fieldType.equals(int.class) || fieldType.equals(Integer.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_INT);
			}
			else if (fieldType.equals(long.class) || fieldType.equals(Long.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_LONG);
			}
			else if (fieldType.equals(float.class) || fieldType.equals(Float.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_FLOAT);
			}
			else if (fieldType.equals(double.class) || fieldType.equals(Double.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_DOUBLE);
			}
			else if (fieldType.equals(boolean.class) || fieldType.equals(Boolean.class)) {
				fieldConfigBuilder.setFieldType(FieldType.BOOL);
			}
			else if (fieldType.equals(Date.class)) {
				fieldConfigBuilder.setFieldType(FieldType.DATE);
			}

			if (f.isAnnotationPresent(IndexedFields.class)) {
				IndexedFields in = f.getAnnotation(IndexedFields.class);
				for (Indexed indexed : in.value()) {
					addIndexedField(indexed, fieldName, fieldConfigBuilder);
				}
			}
			else if (f.isAnnotationPresent(Indexed.class)) {
				Indexed in = f.getAnnotation(Indexed.class);
				addIndexedField(in, fieldName, fieldConfigBuilder);

			}

			if (f.isAnnotationPresent(FacetedFields.class)) {
				FacetedFields ff = f.getAnnotation(FacetedFields.class);
				for (Faceted faceted : ff.value()) {
					addFacetedField(fieldName, fieldConfigBuilder, faceted);
				}
			}
			if (f.isAnnotationPresent(Faceted.class)) {
				Faceted faceted = f.getAnnotation(Faceted.class);
				addFacetedField(fieldName, fieldConfigBuilder, faceted);
			}

			if (f.isAnnotationPresent(SortedFields.class)) {
				SortedFields sf = f.getAnnotation(SortedFields.class);
				for (Sorted sorted : sf.value()) {
					addSortedField(fieldName, fieldConfigBuilder, sorted);
				}
			}
			else if (f.isAnnotationPresent(Sorted.class)) {
				Sorted sorted = f.getAnnotation(Sorted.class);
				addSortedField(fieldName, fieldConfigBuilder, sorted);
			}

			fieldConfigMap.put(fieldName, fieldConfigBuilder.build());
		}

	}

	private void addIndexedField(Indexed in, String fieldName, FieldConfig.Builder fieldConfigBuilder) {
		String analyzerName = in.analyzerName();

		String indexedFieldName = fieldName;
		if (!in.fieldName().isEmpty()) {
			indexedFieldName = in.fieldName();
		}

		IndexAs.Builder builder = IndexAs.newBuilder().setIndexFieldName(indexedFieldName);
		if (!analyzerName.isEmpty()) {
			builder.setAnalyzerName(analyzerName);
		}
		fieldConfigBuilder.addIndexAs(builder);
	}

	private void addFacetedField(String fieldName, FieldConfig.Builder fieldConfigBuilder, Faceted faceted) {
		String facetName = fieldName;
		if (!faceted.name().isEmpty()) {
			facetName = faceted.name();
		}

		FacetAs.DateHandling dateHandling = faceted.dateHandling();

		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName);
		builder.setDateHandling(dateHandling);
		fieldConfigBuilder.addFacetAs(builder);
	}

	private void addSortedField(String fieldName, FieldConfig.Builder fieldConfigBuilder, Sorted sorted) {
		String sortFieldName = fieldName;
		if (!sorted.fieldName().isEmpty()) {
			sortFieldName = sorted.fieldName();
		}
		SortAs.Builder builder = SortAs.newBuilder().setSortFieldName(sortFieldName);
		builder.setStringHandling(sorted.stringHandling());
		fieldConfigBuilder.addSortAs(builder);
	}

	public List<FieldConfig> getFieldConfigs() {
		List<FieldConfig> configs = new ArrayList<>();
		for (String fieldName : fieldConfigMap.keySet()) {
			FieldConfig fieldConfig = fieldConfigMap.get(fieldName);
			configs.add(fieldConfig);
		}
		for (FieldConfigMapper fcm : embeddedFieldConfigMappers) {
			configs.addAll(fcm.getFieldConfigs());
		}
		System.out.println(configs);
		System.out.println();
		return configs;
	}

}
