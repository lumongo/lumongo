package org.lumongo.server.indexing.field;

import org.apache.lucene.facet.taxonomy.directory.LumongoDirectoryTaxonomyWriter;
import org.apache.lucene.index.LumongoIndexWriter;

import java.io.IOException;

/**
 * Created by mdavis on 6/6/15.
 */
public interface IndexWriterManager {
	LumongoIndexWriter getLumongoIndexWriter(int segmentNumber) throws Exception;
	LumongoDirectoryTaxonomyWriter getLumongoDirectoryTaxonomyWriter(int segmentNumber)  throws IOException;
}
