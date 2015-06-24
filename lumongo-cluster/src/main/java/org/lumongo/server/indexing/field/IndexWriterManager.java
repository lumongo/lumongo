package org.lumongo.server.indexing.field;

import org.apache.lucene.index.IndexWriter;

/**
 * Created by mdavis on 6/6/15.
 */
public interface IndexWriterManager {
	IndexWriter getIndexWriter(int segmentNumber) throws Exception;

}
