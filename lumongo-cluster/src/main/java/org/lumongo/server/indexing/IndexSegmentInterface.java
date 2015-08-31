package org.lumongo.server.indexing;

import org.apache.lucene.index.IndexWriter;

/**
 * Created by mdavis on 7/29/15.
 */
public interface IndexSegmentInterface {
	IndexWriter getIndexWriter(int segmentNumber) throws Exception;
}
