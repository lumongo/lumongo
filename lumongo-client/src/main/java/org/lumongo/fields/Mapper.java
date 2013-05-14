package org.lumongo.fields;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.lumongo.LumongoConstants;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Saved;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;
import org.lumongo.util.AnnotationUtil;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Mapper <T> {

	private final Class<T> clazz;

	private HashSet<FactedFieldInfo<T>> facetedFields;
	private HashSet<IndexedFieldInfo<T>> indexedFields;
	private HashSet<SavedFieldInfo<T>> savedFields;

	private UniqueIdFieldInfo<T> uniqueIdField;
	private DefaultSearchFieldInfo<T> defaultSearchField;

	private Settings settings;

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
				Saved saved = f.getAnnotation(Saved.class);

				boolean compressed = saved.compressed();

				if (saved.compressed()) {
					if (String.class.equals(f.getType())) {

					}
					else if (byte[].class.equals(f.getType())) {

					}
					else {
						throw new RuntimeException("Compressed saved field <" + fieldName + "> must a String or byte[] for class <" + clazz.getSimpleName() + ">");
					}
				}

				savedFields.add(new SavedFieldInfo<T>(f, fieldName, compressed));
			}
			if (f.isAnnotationPresent(Faceted.class)) {
				@SuppressWarnings("unused")
				Faceted faceted = f.getAnnotation(Faceted.class);
				facetedFields.add(new FactedFieldInfo<T>(f, fieldName));
			}

			if (f.isAnnotationPresent(UniqueId.class)) {
				@SuppressWarnings("unused")
				UniqueId uniqueId = f.getAnnotation(UniqueId.class);

				if (uniqueIdField == null) {
					uniqueIdField = new UniqueIdFieldInfo<>(f, fieldName);

					if (!String.class.equals(f.getType())) {
						throw new RuntimeException("Unique id field must be a String in class <" + clazz.getSimpleName() + ">");
					}

				}
				else {
					throw new RuntimeException("Cannot define two unique id fields for class <" + clazz.getSimpleName() + ">");
				}

			}

			if (f.isAnnotationPresent(DefaultSearch.class)) {
				@SuppressWarnings("unused")
				DefaultSearch defaultSearch = f.getAnnotation(DefaultSearch.class);

				if (defaultSearchField == null) {
					defaultSearchField = new DefaultSearchFieldInfo<>(f, fieldName);
				}
				else {
					throw new RuntimeException("Cannot define two default search fields for class <" + clazz.getSimpleName() + ">");
				}

			}

		}

		if (uniqueIdField == null) {
			throw new RuntimeException("A unique id field must be defined for class <" + clazz.getSimpleName() + ">");
		}

		if (defaultSearchField == null) {
			throw new RuntimeException("A default search field must be defined for class <" + clazz.getSimpleName() + ">");
		}

		if (clazz.isAnnotationPresent(Settings.class)) {
			settings = clazz.getAnnotation(Settings.class);
		}
	}

	public CreateOrUpdateIndex createOrUpdateIndex() {

		if (settings == null) {
			throw new RuntimeException("No Settings annonation for class <" + clazz.getSimpleName() + ">");
		}

		IndexConfig indexConfig = new IndexConfig(defaultSearchField.getFieldName());

		for (IndexedFieldInfo<T> ifi : indexedFields) {
			indexConfig.setFieldAnalyzer(ifi.getFieldName(), ifi.getLMAnalyzer());
		}

		indexConfig.setDefaultAnalyzer(settings.defaultAnalyzer());
		indexConfig.setApplyUncommitedDeletes(settings.applyUncommitedDeletes());
		indexConfig.setRequestFactor(settings.requestFactor());
		indexConfig.setMinSegmentRequest(settings.minSeqmentRequest());
		indexConfig.setIdleTimeWithoutCommit(settings.idleTimeWithoutCommit());
		indexConfig.setSegmentCommitInterval(settings.segmentCommitInterval());
		indexConfig.setBlockCompression(settings.blockCompression());
		indexConfig.setSegmentTolerance(settings.segmentTolerance());
		indexConfig.setSegmentFlushInterval(settings.segmentFlushInterval());


		CreateOrUpdateIndex createOrUpdateIndex = new CreateOrUpdateIndex(settings.indexName(), settings.numberOfSegments(), uniqueIdField.getFieldName(),
				indexConfig);
		createOrUpdateIndex.setDatabasePerIndexSegment(settings.databasePerIndexSegment());
		createOrUpdateIndex.setDatabasePerRawDocumentSegment(settings.databasePerRawDocumentSegment());
		createOrUpdateIndex.setCollectionPerRawDocumentSegment(settings.collectionPerRawDocumentSegment());

		// TODO consider this
		// indexCreateRequestBuilder.setFaceted(!facetedFields.isEmpty());
		createOrUpdateIndex.setFaceted(true);

		return createOrUpdateIndex;
	}


	public Class<T> getClazz() {
		return clazz;
	}

	public Store createStore(T object) throws Exception {
		if (settings == null) {
			throw new RuntimeException("No Settings annonation for class <" + clazz.getSimpleName() + ">");
		}
		return createStore(settings.indexName(), object);
	}

	public Store createStore(String indexName, T object) throws Exception {
		LMDoc lmDoc = toLMDoc(object);
		ResultDocBuilder rd = toResultDocumentBuilder(object);
		Store store = new Store(rd.getUniqueId(), indexName);
		store.setResultDocument(rd);
		store.setIndexedDocument(lmDoc);
		return store;
	}

	public List<T> fromBatchFetchResult(BatchFetchResult batchFetchResult) throws Exception {
		List<T> results = new ArrayList<T>();
		for (FetchResult fr : batchFetchResult.getFetchResults()) {
			results.add(fr.getDocument(this));
		}
		return results;
	}

	public T fromFetchResult(FetchResult fetchResult) throws Exception {
		return fetchResult.getDocument(this);
	}

	public LMDoc toLMDoc(T object) throws Exception {

		LMDoc.Builder lmBuilder = LMDoc.newBuilder();

		for (IndexedFieldInfo<T> ifi : indexedFields) {
			LMField lmField = ifi.build(object);
			if (lmField != null) {
				lmBuilder.addIndexedField(lmField);
			}
		}

		for (FactedFieldInfo<T> ffi : facetedFields) {
			List<String> values = ffi.build(object);

			for (String value : values) {
				lmBuilder.addFacet(ffi.getFacetPrefix() + LumongoConstants.FACET_DELIMITER + value);

			}

		}

		return lmBuilder.build();
	}

	public ResultDocBuilder toResultDocumentBuilder(T object) throws Exception {
		String uniqueId = uniqueIdField.build(object);
		DBObject document = new BasicDBObject();
		for (SavedFieldInfo<T> sfi : savedFields) {
			Object o = sfi.getValue(object);
			document.put(sfi.getFieldName(), o);
		}
		ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder();
		resultDocumentBuilder.setDocument(document).setUniqueId(uniqueId);
		return resultDocumentBuilder;
	}


	public T fromDBObject(DBObject savedDBObject) throws Exception {
		T newInstance = clazz.newInstance();
		for (SavedFieldInfo<T> sfi : savedFields) {
			sfi.populate(newInstance, savedDBObject);
		}

		uniqueIdField.populate(newInstance, savedDBObject);

		return newInstance;
	}



}
