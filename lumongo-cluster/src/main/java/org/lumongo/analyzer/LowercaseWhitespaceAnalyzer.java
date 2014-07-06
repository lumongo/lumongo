package org.lumongo.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.lumongo.LumongoLuceneConstants;

public class LowercaseWhitespaceAnalyzer extends Analyzer {
	public LowercaseWhitespaceAnalyzer() {
	}
	
	@Override
	protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		
		WhitespaceTokenizer src = new WhitespaceTokenizer(LumongoLuceneConstants.VERSION, reader);
		TokenStream tok = new LowerCaseFilter(LumongoLuceneConstants.VERSION, src);
		
		return new TokenStreamComponents(src, tok);
	}
	
}
