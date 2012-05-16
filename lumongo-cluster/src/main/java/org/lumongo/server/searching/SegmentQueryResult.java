package org.lumongo.server.searching;

import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;

public class SegmentQueryResult {
	
	private ScoredResult[] results;
	private int totalHits;
	private boolean moreAvailable;
	private int index;
	private int segmentNumber;
	private String indexName;
	
	public SegmentQueryResult(SegmentResponse sr) {
		this.segmentNumber = sr.getSegmentNumber();
		this.results = sr.getScoredResultList().toArray(new ScoredResult[0]);
		this.totalHits = sr.getTotalHits();
		this.index = -1;
		this.moreAvailable = sr.getMoreAvailable();
		this.indexName = sr.getIndexName();
	}
	
	public int getIndex() {
		return index;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public ScoredResult getCurrent() {
		if (index != -1) {
			return this.results[index];
		}
		return null;
	}
	
	public boolean moreAvailable() {
		return moreAvailable;
	}
	
	public boolean hasNext() {
		return (index + 1) < results.length;
	}
	
	public ScoredResult previewNext() {
		if ((index + 1) < results.length) {
			return this.results[index + 1];
		}
		return null;
	}
	
	public ScoredResult next() {
		++index;
		if (index < results.length) {
			return this.results[index];
		}
		return null;
	}
	
	public void reset() {
		index = 0;
	}
	
	public int getTotalHits() {
		return totalHits;
	}
	
	public void setTotalHits(int totalHits) {
		this.totalHits = totalHits;
	}
	
	public int getReturnedHits() {
		return results.length;
	}
	
	public int getSegmentNumber() {
		return segmentNumber;
	}
	
	public void setSegmentNumber(int segmentNumber) {
		this.segmentNumber = segmentNumber;
	}
	
}
