package org.lumongo.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import org.lumongo.LuceneConstants;

public class LowercaseWhitespaceAnalyzer extends Analyzer {
    public LowercaseWhitespaceAnalyzer() {
    }
	
	@Override
	protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		
		WhitespaceTokenizer src = new WhitespaceTokenizer(LuceneConstants.VERSION, reader);
		TokenStream tok = new LowerCaseFilter(LuceneConstants.VERSION, src);
		
		return new TokenStreamComponents(src, tok);
	}
	
}
