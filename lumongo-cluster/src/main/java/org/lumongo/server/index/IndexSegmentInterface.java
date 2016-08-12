package org.lumongo.server.index;

import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;

/**
 * Created by Matt Davis on 7/29/15.
 * @author mdavis
 */
public interface IndexSegmentInterface {
	IndexWriter getIndexWriter(int segmentNumber) throws Exception;

	PerFieldAnalyzerWrapper getPerFieldAnalyzer() throws Exception;

	DirectoryTaxonomyWriter getTaxoWriter(int segmentNumber) throws Exception;
}
