package org.lumongo.util;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockHandler {
	
	private LockIndexer segmentIndexer;
	private ReadWriteLock[] readWriteLock;
	
	public LockHandler() {
		this.segmentIndexer = new LockIndexer(10);
		
		readWriteLock = new ReadWriteLock[segmentIndexer.getSegmentSize()];
		for (int i = 0; i < segmentIndexer.getSegmentSize(); i++) {
			readWriteLock[i] = new ReentrantReadWriteLock();
		}
	}
	
	public ReadWriteLock getLock(String uniqueId) {
		int h = uniqueId.hashCode();
		int index = segmentIndexer.getIndex(h);
		return readWriteLock[index];
	}

	public ReadWriteLock getLock(long uniqueId) {
		int h = Long.hashCode(uniqueId);
		int index = segmentIndexer.getIndex(h);
		return readWriteLock[index];
	}

	public ReadWriteLock getLock(Long uniqueId) {
		int h = uniqueId.hashCode();
		int index = segmentIndexer.getIndex(h);
		return readWriteLock[index];
	}
}
