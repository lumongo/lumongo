package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;

public class LumongoIndexWriter extends IndexWriter {

    public LumongoIndexWriter(Directory d, IndexWriterConfig conf) throws CorruptIndexException,
            LockObtainFailedException, IOException {
        super(d, conf.setReaderPooling(true));
    }

    DirectoryReader getReader(boolean applyAllDeletes) throws IOException {
        return getReader(applyAllDeletes, true);
    }

    public DirectoryReader getReader(boolean applyAllDeletes, boolean realTime) throws IOException {

        if (realTime) {
            return super.getReader(applyAllDeletes);
        }

        ensureOpen();

        final DirectoryReader r;

        //TODO this might not be safe enough because not inside fullFlushLock and not handling OOM error
        //might have to import more of the IndexWriter class or drop non real time functionality
        synchronized (this) {
            maybeApplyDeletes(applyAllDeletes);
            r = StandardDirectoryReader.open(this, segmentInfos, applyAllDeletes);
        }

        return r;
    }

    public void flush(boolean applyAllDeletes) throws CorruptIndexException, IOException {
        flush(false, applyAllDeletes);
    }

}
