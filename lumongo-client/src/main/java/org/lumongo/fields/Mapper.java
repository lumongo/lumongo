package org.lumongo.fields;

import com.mongodb.DBObject;
import org.lumongo.client.command.BatchFetch;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Embedded;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.IndexedFields;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;
import org.lumongo.util.AnnotationUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Mapper<T> {

	private final Class<T> clazz;

	private SavedFieldsMapper<T> savedFieldsMapper;

	private FieldConfigMapper<T> fieldConfigMapper;

	private UniqueIdFieldInfo<T> uniqueIdField;

	private DefaultSearchFieldInfo<T> defaultSearchField;

	private Settings settings;

	public Mapper(Class<T> clazz) {

		this.clazz = clazz;

		this.savedFieldsMapper = new SavedFieldsMapper<>(clazz);

		this.fieldConfigMapper = new FieldConfigMapper<>(clazz, "");

		HashSet<String> fields = new HashSet<>();

		List<Field> allFields = AnnotationUtil.getNonStaticFields(clazz, true);

		for (Field f : allFields) {
			f.setAccessible(true);

			String fieldName = f.getName();

			savedFieldsMapper.setupField(f);
			fieldConfigMapper.setupField(f);

			if (f.isAnnotationPresent(UniqueId.class)) {

				if (f.isAnnotationPresent(AsField.class)) {
					throw new RuntimeException("Cannot use AsField with UniqueId on field <" + f.getName() + "> for class <" + clazz.getSimpleName()
									+ ">.  Unique id always stored as _id.");
				}
				if (f.isAnnotationPresent(Embedded.class)) {
					throw new RuntimeException(
									"Cannot use Embedded with UniqueId with on field <" + f.getName() + "> for class <" + clazz.getSimpleName() + ">");
				}

				@SuppressWarnings("unused") UniqueId uniqueId = f.getAnnotation(UniqueId.class);

				fieldName = "_id";

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

				if (!f.isAnnotationPresent(Indexed.class) && !f.isAnnotationPresent(IndexedFields.class)) {
					throw new RuntimeException("DefaultSearch must be on Indexed field <" + f.getName() + "> for class <" + clazz.getSimpleName() + ">");
				}

				@SuppressWarnings("unused") DefaultSearch defaultSearch = f.getAnnotation(DefaultSearch.class);

				if (defaultSearchField == null) {
					defaultSearchField = new DefaultSearchFieldInfo<>(f, fieldName);
				}
				else {
					throw new RuntimeException("Cannot define two default search fields for class <" + clazz.getSimpleName() + ">");
				}

			}

			if (fields.contains(fieldName)) {
				throw new RuntimeException("Duplicate field name <" + fieldName + ">");
			}
			fields.add(fieldName);

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
			throw new RuntimeException("No Settings annotation for class <" + clazz.getSimpleName() + ">");
		}

		IndexConfig indexConfig = new IndexConfig(defaultSearchField.getFieldName());

		indexConfig.setApplyUncommitedDeletes(settings.applyUncommitedDeletes());
		indexConfig.setRequestFactor(settings.requestFactor());
		indexConfig.setMinSegmentRequest(settings.minSeqmentRequest());
		indexConfig.setIdleTimeWithoutCommit(settings.idleTimeWithoutCommit());
		indexConfig.setSegmentCommitInterval(settings.segmentCommitInterval());
		indexConfig.setBlockCompression(settings.blockCompression());
		indexConfig.setSegmentTolerance(settings.segmentTolerance());
		indexConfig.setSegmentFlushInterval(settings.segmentFlushInterval());
		indexConfig.setSegmentQueryCacheSize(settings.segmentQueryCacheSize());
		indexConfig.setSegmentQueryCacheMaxAmount(settings.segmentQueryCacheMaxAmount());

		for (Lumongo.FieldConfig fieldConfig : fieldConfigMapper.getFieldConfigs()) {
			indexConfig.addFieldConfig(fieldConfig);
		}

		return new CreateOrUpdateIndex(settings.indexName(), settings.numberOfSegments(), uniqueIdField.getFieldName(), indexConfig);
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
		ResultDocBuilder rd = toResultDocumentBuilder(object);
		Store store = new Store(rd.getUniqueId(), indexName);
		store.setResultDocument(rd);
		return store;
	}

	public List<T> fromQueryResult(LumongoWorkPool lumongoWorkPool, QueryResult queryResult) throws Exception {
		BatchFetch batchFetch = new BatchFetch().addFetchDocumentsFromResults(queryResult);
		BatchFetchResult bfr = lumongoWorkPool.batchFetch(batchFetch);
		return fromBatchFetchResult(bfr);
	}

	public List<T> fromBatchFetchResult(BatchFetchResult batchFetchResult) throws Exception {
		List<T> results = new ArrayList<>();
		for (FetchResult fr : batchFetchResult.getFetchResults()) {
			results.add(fr.getDocument(this));
		}
		return results;
	}

	public T fromFetchResult(FetchResult fetchResult) throws Exception {
		return fetchResult.getDocument(this);
	}

	public ResultDocBuilder toResultDocumentBuilder(T object) throws Exception {
		String uniqueId = uniqueIdField.build(object);
		DBObject document = toDbObject(object);
		ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder();
		resultDocumentBuilder.setDocument(document).setUniqueId(uniqueId);
		return resultDocumentBuilder;
	}

	public DBObject toDbObject(T object) throws Exception {
		return savedFieldsMapper.toDbObject(object);
	}

	public T fromDBObject(DBObject savedDBObject) throws Exception {
		T newInstance = savedFieldsMapper.fromDBObject(savedDBObject);
		uniqueIdField.populate(newInstance, savedDBObject);
		return newInstance;
	}

}
