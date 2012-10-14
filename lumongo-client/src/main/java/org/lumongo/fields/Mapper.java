package org.lumongo.fields;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.lumongo.LumongoConstants;
import org.lumongo.client.command.Store;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexCreateRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Saved;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;
import org.lumongo.util.AnnotationUtil;
import org.lumongo.util.BsonHelper;

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
                    if (!String.class.equals(f.getType())) {
                        throw new RuntimeException("Compressed saved field <" + fieldName + "> must a String for class <" + clazz.getSimpleName() + ">");
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

    public IndexCreateRequest getIndexCreateRequest() {

        if (settings == null) {
            throw new RuntimeException("No Settings annonation for class <" + clazz.getSimpleName() + ">");
        }

        IndexCreateRequest.Builder indexCreateRequestBuilder = IndexCreateRequest.newBuilder();
        indexCreateRequestBuilder.setIndexName(settings.indexName());
        indexCreateRequestBuilder.setNumberOfSegments(settings.numberOfSegments());
        indexCreateRequestBuilder.setUniqueIdField(uniqueIdField.getFieldName());

        // TODO consider this
        // indexCreateRequestBuilder.setFaceted(!facetedFields.isEmpty());
        indexCreateRequestBuilder.setFaceted(true);

        IndexSettings.Builder indexSettingsBuilder = IndexSettings.newBuilder();

        indexSettingsBuilder.setDefaultSearchField(defaultSearchField.getFieldName());

        indexSettingsBuilder.addAllFieldConfig(getFieldConfig());
        indexSettingsBuilder.setDefaultAnalyzer(settings.defaultAnalyzer());
        indexSettingsBuilder.setApplyUncommitedDeletes(settings.applyUncommitedDeletes());
        indexSettingsBuilder.setRequestFactor(settings.requestFactor());
        indexSettingsBuilder.setMinSegmentRequest(settings.minSeqmentRequest());
        indexSettingsBuilder.setIdleTimeWithoutCommit(settings.idleTimeWithoutCommit());
        indexSettingsBuilder.setSegmentCommitInterval(settings.segmentCommitInterval());
        indexSettingsBuilder.setBlockCompression(settings.blockCompression());
        indexSettingsBuilder.setSegmentTolerance(settings.segmentTolerance());
        indexSettingsBuilder.setSegmentFlushInterval(settings.segmentFlushInterval());

        indexCreateRequestBuilder.setIndexSettings(indexSettingsBuilder);

        return indexCreateRequestBuilder.build();
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

    public Store createStore(String index, T object) throws Exception {
        LMDoc lmDoc = toLMDoc(index, object);
        ResultDocument rd = toResultDocument(object);
        Store store = new Store(rd.getUniqueId());
        store.setResultDocument(rd);
        store.addIndexedDocument(lmDoc);
        return store;
    }

    public LMDoc toLMDoc(String index, T object) throws IllegalArgumentException, IllegalAccessException {

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
                lmBuilder.addFacet(ffi.getFacetPrefix() + LumongoConstants.FACET_DELIMITER + value);

            }

        }

        return lmBuilder.build();
    }

    public ResultDocument toResultDocument(T object) throws Exception {
        String uniqueId = uniqueIdField.build(object);
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

    public T fromResultDocument(ResultDocument rd) throws Exception {
        T newInstance = clazz.newInstance();

        DBObject savedDBObject = BsonHelper.dbObjectFromResultDocument(rd);
        for (SavedFieldInfo<T> sfi : savedFields) {
            sfi.populate(newInstance, savedDBObject);
        }

        return newInstance;
    }



}
