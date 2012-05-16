package org.lumongo.server.exceptions;

public class SegmentDoesNotExist extends Exception {
	
	private static final long serialVersionUID = 1L;
	private String indexName;
	private int segmentNumber;
	
	public SegmentDoesNotExist(String indexName, int segmentNumber) {
		this.indexName = indexName;
		this.segmentNumber = segmentNumber;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public int getSegmentNumber() {
		return segmentNumber;
	}
}
