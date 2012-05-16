package org.lumongo.server.exceptions;

public class IndexDoesNotExist extends Exception {
	
	private static final long serialVersionUID = 1L;
	private String indexName;
	
	public IndexDoesNotExist(String indexName) {
		super("Index <" + indexName + "> does not exist");
		this.indexName = indexName;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
}
