package org.apache.lucene.analysis.lsh;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * Created by mdavis on 1/20/16.
 */
public class LSHAnalyzer extends Analyzer {

	public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;

	private final int numHash;

	public LSHAnalyzer(int numHash) {
		this.numHash = numHash;
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		StandardTokenizer tokenizer = new StandardTokenizer();

		TokenStream tok = new StandardFilter(tokenizer);
		tok = new LowerCaseFilter(tok);
		tok = new StopFilter(tok, STOP_WORDS_SET);
		tok = new ASCIIFoldingFilter(tok);

		final TokenStream stream = new MinHashFilter(tok, numHash);

		return new TokenStreamComponents(tokenizer, stream);
	}
}
