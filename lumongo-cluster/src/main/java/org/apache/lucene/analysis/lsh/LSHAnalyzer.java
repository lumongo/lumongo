package org.apache.lucene.analysis.lsh;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Created by mdavis on 1/20/16.
 */
public class LSHAnalyzer extends Analyzer {

	private final int numHash;

	public LSHAnalyzer(int numHash) {
		this.numHash = numHash;
	}
	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		StandardTokenizer tokenizer = new StandardTokenizer();
		final TokenStream stream = new MinHashFilter(tokenizer, numHash);
		return new TokenStreamComponents(tokenizer, stream);
	}
}
