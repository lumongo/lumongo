package org.apache.lucene.index;

import org.apache.lucene.store.Directory;

import java.io.IOException;

public class LumongoIndexWriter extends IndexWriter {

	public LumongoIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
		super(d, conf.setReaderPooling(true));
	}

	@Override
	DirectoryReader getReader(boolean applyAllDeletes) throws IOException {
		return getReader(applyAllDeletes, true);
	}

	public DirectoryReader getReader(boolean applyAllDeletes, boolean realTime) throws IOException {

		if (realTime) {
			return super.getReader(applyAllDeletes);
		}

		ensureOpen();

		final DirectoryReader r;

		//TODO this might not be safe enough because not inside fullFlushLock and it is not handling OOM error
		//might have to import more of the IndexWriter class or drop non real time functionality
		synchronized (this) {
			maybeApplyDeletes(applyAllDeletes);
			r = StandardDirectoryReader.open(this, segmentInfos, applyAllDeletes);
		}

		return r;
	}

	public void flush(boolean applyAllDeletes) throws IOException {
		flush(false, applyAllDeletes);
	}

}
