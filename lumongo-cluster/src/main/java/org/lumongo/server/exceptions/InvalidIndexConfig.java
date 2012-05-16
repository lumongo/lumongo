package org.lumongo.server.exceptions;

public class InvalidIndexConfig extends Exception {
	
	private static final long serialVersionUID = 1L;
	private String indexName;
	
	public InvalidIndexConfig(String indexName, String message) {
		super("Index <" + indexName + "> has invalid configuration: " + message);
		this.indexName = indexName;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
}
