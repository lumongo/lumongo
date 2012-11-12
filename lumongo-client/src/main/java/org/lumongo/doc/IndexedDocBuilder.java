package org.lumongo.doc;

import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.util.StringUtil;

public class IndexedDocBuilder {

    private LMDoc.Builder indexedDocBuilder;

    public IndexedDocBuilder(String indexName) {
        indexedDocBuilder = LMDoc.newBuilder();
        indexedDocBuilder.setIndexName(indexName);
    }

    public String getIndexName() {
        return indexedDocBuilder.getIndexName();
    }

    public IndexedDocBuilder addField(String fieldName, String... values) {

        for (String value : values) {
            indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addFieldValue(value));
        }

        return this;
    }

    public IndexedDocBuilder addField(String fieldName, Integer... values) {

        for (Integer value : values) {
            indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addIntValue(value));
        }

        return this;
    }

    public IndexedDocBuilder addField(String fieldName, Float... values) {

        for (Float value : values) {
            indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addFloatValue(value));
        }
        return this;
    }

    public IndexedDocBuilder addField(String fieldName, Double... values) {

        for (Double value : values) {
            indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName(fieldName).addDoubleValue(value));
        }
        return this;
    }

    public void addFacet(String... path) {
        indexedDocBuilder.addFacet(StringUtil.join(LumongoConstants.FACET_DELIMITER, path));
    }

    public LMDoc getIndexedDoc() {
        return indexedDocBuilder.build();
    }


}
