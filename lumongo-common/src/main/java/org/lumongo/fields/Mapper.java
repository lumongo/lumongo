package org.lumongo.fields;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.StoreRequest;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Saved;
import org.lumongo.util.AnnotationUtil;
import org.lumongo.util.BsonHelper;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Mapper <T> {

	private final Class<T> clazz;

	private HashSet<FactedFieldInfo<T>> facetedFields;
	private HashSet<IndexedFieldInfo<T>> indexedFields;
	private HashSet<SavedFieldInfo<T>> savedFields;

	public Mapper(Class<T> clazz) {
		this.facetedFields = new HashSet<FactedFieldInfo<T>>();
		this.indexedFields = new HashSet<IndexedFieldInfo<T>>();
		this.savedFields = new HashSet<SavedFieldInfo<T>>();

		this.clazz = clazz;

		HashSet<Field> allFields = AnnotationUtil.getNonStaticFields(clazz, true);

		for (Field f : allFields) {
			f.setAccessible(true);

			String fieldName = f.getName();

			LMAnalyzer lma = null;

			if (f.isAnnotationPresent(AsField.class)) {
				AsField as = f.getAnnotation(AsField.class);
				fieldName = as.value();
			}

			if (f.isAnnotationPresent(Indexed.class)) {
				Indexed in = f.getAnnotation(Indexed.class);
				lma = in.value();
				indexedFields.add(new IndexedFieldInfo<T>(f, fieldName, lma));
			}
			if (f.isAnnotationPresent(Saved.class)) {
				@SuppressWarnings("unused")
				Saved saved = f.getAnnotation(Saved.class);
				savedFields.add(new SavedFieldInfo<T>(f, fieldName));
			}
			if (f.isAnnotationPresent(Faceted.class)) {
				@SuppressWarnings("unused")
				Faceted faceted = f.getAnnotation(Faceted.class);
				facetedFields.add(new FactedFieldInfo<T>(f, fieldName));
			}

		}
	}

	public Class<T> getClazz() {
		return clazz;
	}

	public StoreRequest toStoreRequest(String index, String uniqueId, T object) throws IllegalArgumentException, IllegalAccessException {
		LMDoc lmDoc = toLMDoc(index, uniqueId, object);
		ResultDocument rd = toResultDocument(uniqueId, object);
		StoreRequest.Builder storeBuilder = StoreRequest.newBuilder();
		storeBuilder.setResultDocument(rd);
		storeBuilder.addIndexedDocument(lmDoc);
		return storeBuilder.build();

	}

	public LMDoc toLMDoc(String index, String uniqueId, T object) throws IllegalArgumentException, IllegalAccessException {

		LMDoc.Builder lmBuilder = LMDoc.newBuilder();
		lmBuilder.setIndexName(index);

		for (IndexedFieldInfo<T> ifi : indexedFields) {
			LMField lmField = ifi.build(object);
			if (lmField != null) {
				lmBuilder.addIndexedField(lmField);
			}
		}

		for (FactedFieldInfo<T> ffi : facetedFields) {
			List<String> values = ffi.build(object);

			for (String value : values) {
				lmBuilder.addFacet(ffi.getFacetPrefix() + value);
			}

		}

		return lmBuilder.build();
	}

	public ResultDocument toResultDocument(String uniqueId, T object) throws IllegalArgumentException, IllegalAccessException {
		DBObject document = new BasicDBObject();
		for (SavedFieldInfo<T> sfi : savedFields) {
			Object o = sfi.getValue(object);
			document.put(sfi.getFieldName(), o);
		}
		return BsonHelper.dbObjectToResultDocument(uniqueId, document);
	}

	public List<FieldConfig> getFieldConfig() {

		List<FieldConfig> results = new ArrayList<FieldConfig>();

		for (IndexedFieldInfo<T> ifi : indexedFields) {
			FieldConfig.Builder fc = FieldConfig.newBuilder();
			fc.setAnalyzer(ifi.getLMAnalyzer());
			fc.setFieldName(ifi.getFieldName());
			results.add(fc.build());

		}

		return results;

	}



}
