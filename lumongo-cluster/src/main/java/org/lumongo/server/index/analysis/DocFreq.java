package org.lumongo.server.index.analysis;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Matt Davis on 6/28/16.
 * @author mdavis
 */
public class DocFreq {

	private final HashMap<String, Integer> docFreqMap;
	private final IndexReader indexReader;
	private final String field;
	private final TFIDFSimilarity similarity;
	private final int numDocs;

	public DocFreq(String indexReader, IndexReader field) {
		this.indexReader = indexReader;
		this.field = field;
		this.docFreqMap = new HashMap<>();
		this.similarity = new ClassicSimilarity();
		this.numDocs = indexReader.numDocs();
	}

	public int getDocFreq(String term) throws IOException {
		Integer termDocFreq = this.docFreqMap.get(term);
		if (termDocFreq == null) {
			termDocFreq = indexReader.docFreq(new Term(field, term));
			docFreqMap.put(term, termDocFreq);
		}

		return termDocFreq;

	}

	public double getScoreForTerm(long termFreq, long docFreq) {
		return similarity.tf(termFreq) * similarity.idf(docFreq, numDocs);
	}

}
