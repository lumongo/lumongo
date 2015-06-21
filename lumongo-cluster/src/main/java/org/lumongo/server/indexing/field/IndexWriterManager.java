package org.lumongo.server.indexing.field;

import org.apache.lucene.index.LumongoIndexWriter;

/**
 * Created by mdavis on 6/6/15.
 */
public interface IndexWriterManager {
	LumongoIndexWriter getLumongoIndexWriter(int segmentNumber) throws Exception;

}
