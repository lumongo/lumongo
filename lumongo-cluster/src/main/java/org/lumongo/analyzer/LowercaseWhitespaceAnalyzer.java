package org.lumongo.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import java.io.Reader;

public class LowercaseWhitespaceAnalyzer extends Analyzer {
	public LowercaseWhitespaceAnalyzer() {
	}
	
	@Override
	protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		
		WhitespaceTokenizer src = new WhitespaceTokenizer(reader);
		TokenStream tok = new LowerCaseFilter(src);
		
		return new TokenStreamComponents(src, tok);
	}
	
}
