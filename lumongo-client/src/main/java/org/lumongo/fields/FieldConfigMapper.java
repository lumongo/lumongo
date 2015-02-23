package org.lumongo.fields;

import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.annotations.*;
import org.lumongo.util.AnnotationUtil;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FieldConfigMapper<T> {

	private final String prefix;

	private final Class<T> clazz;

	private HashMap<String, Lumongo.FieldConfig> fieldConfigMap;

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


		if (f.isAnnotationPresent(Embedded.class)) {
			if (f.isAnnotationPresent(IndexedFields.class) || f.isAnnotationPresent(Indexed.class) || f.isAnnotationPresent(Faceted.class) || f
							.isAnnotationPresent(UniqueId.class) || f.isAnnotationPresent(DefaultSearch.class)) {
				throw new RuntimeException(
								"Cannot use Indexed, Faceted, UniqueId, DefaultSearch on embedded field <" + f.getName() + "> for class <"
												+ clazz.getSimpleName() + ">");
			}

			Class<?> type = f.getType();

			if (List.class.isAssignableFrom(type)) {
				Type genericType = f.getGenericType();
				if (genericType instanceof ParameterizedType) {
					ParameterizedType pType = (ParameterizedType)genericType;
					type = (Class<?>)pType.getActualTypeArguments()[0];
				}
			}

			FieldConfigMapper fieldConfigMapper = new FieldConfigMapper<>(type, fieldName);

			List<Field> allFields = AnnotationUtil.getNonStaticFields(type, true);

			for (Field ef : allFields) {
				ef.setAccessible(true);
				fieldConfigMapper.setupField(ef);
			}
			embeddedFieldConfigMappers.add(fieldConfigMapper);
		}
		else {
			Lumongo.FieldConfig.Builder fieldConfigBuilder = Lumongo.FieldConfig.newBuilder();
			fieldConfigBuilder.setStoredFieldName(fieldName);

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

			if (f.isAnnotationPresent(Faceted.class)) {
				Faceted faceted = f.getAnnotation(Faceted.class);

				String facetName = fieldName;
				if (!faceted.name().isEmpty()) {
					facetName = faceted.name();
				}

				Lumongo.FacetAs.LMFacetType facetType = faceted.type();

				fieldConfigBuilder.addFacetAs(Lumongo.FacetAs.newBuilder().setFacetName(facetName).setFacetType(facetType));
			}
			fieldConfigMap.put(fieldName, fieldConfigBuilder.build());
		}




	}

	private void addIndexedField(Indexed in, String fieldName, Lumongo.FieldConfig.Builder fieldConfigBuilder) {
		Lumongo.LMAnalyzer analyzer = in.analyzer();

		String indexedFieldName = fieldName;
		if (!in.fieldName().isEmpty()) {
			indexedFieldName = in.fieldName();
		}

		fieldConfigBuilder.addIndexAs(Lumongo.IndexAs.newBuilder().setIndexFieldName(indexedFieldName).setAnalyzer(analyzer));
	}

	public List<Lumongo.FieldConfig> getFieldConfigs() {
		List<Lumongo.FieldConfig> configs = new ArrayList<>();
		for (String fieldName : fieldConfigMap.keySet()) {
			Lumongo.FieldConfig fieldConfig = fieldConfigMap.get(fieldName);
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
