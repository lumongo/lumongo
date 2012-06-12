package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;

public class LumongoIndexWriter extends IndexWriter {
	
	public LumongoIndexWriter(Directory d, IndexWriterConfig conf) throws CorruptIndexException, LockObtainFailedException, IOException {
		super(d, conf.setReaderPooling(true));
	}
	
	@Override
	IndexReader getReader(int termInfosIndexDivisor, boolean applyAllDeletes) throws IOException {
		return getReader(termInfosIndexDivisor, applyAllDeletes, true);
	}
	
	public IndexReader getReader(boolean applyAllDeletes, boolean realTime) throws IOException {
		return getReader(IndexReader.DEFAULT_TERMS_INDEX_DIVISOR, applyAllDeletes, realTime);
	}
	
	public IndexReader getReader(int termInfosIndexDivisor, boolean applyAllDeletes, boolean realTime) throws IOException {
		ensureOpen();
		
		// Prevent segmentInfos from changing while opening the
		// reader; in theory we could do similar retry logic,
		// just like we do when loading segments_N
		IndexReader r;
		synchronized (this) {
			if (realTime) {
				flush(false, applyAllDeletes);
			}
			r = new ReadOnlyDirectoryReader(this, segmentInfos, termInfosIndexDivisor, applyAllDeletes);
			
		}
		
		if (realTime) {
			maybeMerge();
		}
		
		return r;
	}
	
	public void flush(boolean applyAllDeletes) throws CorruptIndexException, IOException {
		flush(false, applyAllDeletes);
	}
	
}
