package org.apache.lucene.facet.taxonomy.directory;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LumongoIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.IOException;

public class LumongoDirectoryTaxonomyWriter extends DirectoryTaxonomyWriter {

	private LumongoIndexWriter myIndexWriter;

	public LumongoDirectoryTaxonomyWriter(Directory d) throws CorruptIndexException, LockObtainFailedException, IOException {
		super(d);

	}

	@Override
	protected LumongoIndexWriter openIndexWriter(Directory directory, IndexWriterConfig config) throws IOException {
		// Make sure we use a MergePolicy which merges segments in-order and thus
		// keeps the doc IDs ordered as well (this is crucial for the taxonomy
		// index).

		//use lumongo index writer to have control over real time flushes
		myIndexWriter = new LumongoIndexWriter(directory, config);
		return myIndexWriter;
	}

	public DirectoryReader openReader(boolean realTime) throws IOException {
		return myIndexWriter.getReader(false, realTime);
	}

	public void flush() throws IOException {
		myIndexWriter.flush(false);
	}

	public LumongoIndexWriter getLumongoIndexWriter() {
		return myIndexWriter;
	}

}
