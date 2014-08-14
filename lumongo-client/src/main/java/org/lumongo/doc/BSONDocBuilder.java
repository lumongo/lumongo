package org.lumongo.doc;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.lumongo.client.command.Store;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class BSONDocBuilder extends IndexedDocBuilder {
	
	protected ResultDocBuilder resultDocumentBuilder;

	protected DBObject dbObject;
	
	public BSONDocBuilder() {
		resultDocumentBuilder = new ResultDocBuilder();
		dbObject = new BasicDBObject();
	}

	public BSONDocBuilder addStoredField(String fieldName, String... values) {
		super.addField(fieldName, values);
		
		addBase(fieldName, (Object[]) values);

		return this;
	}
	
	public BSONDocBuilder addStoredField(String fieldName, Integer... values) {
		super.addField(fieldName, values);
		
		addBase(fieldName, (Object[]) values);

		return this;
	}

	public BSONDocBuilder addStoredField(String fieldName, Long... values) {
		super.addField(fieldName, values);
		
		addBase(fieldName, (Object[]) values);

		return this;
	}
	
	public BSONDocBuilder addStoredField(String fieldName, Float... values) {
		super.addField(fieldName, values);
		
		addBase(fieldName, (Object[]) values);

		return this;
	}

	public BSONDocBuilder addStoredField(String fieldName, Double... values) {
		super.addField(fieldName, values);
		
		addBase(fieldName, (Object[]) values);

		return this;
	}

	public BSONDocBuilder addStoredField(String fieldName, Date... values) {
		super.addField(fieldName, values);
		
		addBase(fieldName, (Object[]) values);

		return this;
	}

	protected void addBase(String fieldName, Object... values) {
		@SuppressWarnings("unchecked")
		List<Object> currentValues = (List<Object>) dbObject.get(fieldName);
		if (currentValues == null) {
			currentValues = new ArrayList<>();
			dbObject.put(fieldName, currentValues);
		}
		
		for (Object value : values) {
			currentValues.add(value);
		}
	}

	public Store getStore(String id, String indexName) {
		Store store = new Store(id, indexName);
		store.setIndexedDocument(getIndexedDoc());
		resultDocumentBuilder.setDocument(dbObject);
		store.setResultDocument(resultDocumentBuilder);
		return store;
	}
}
