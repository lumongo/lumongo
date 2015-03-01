package org.lumongo.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;

import java.io.Reader;

public class LowercaseKeywordAnalyzer extends Analyzer {
	public LowercaseKeywordAnalyzer() {
	}
	
	@Override
	protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		KeywordTokenizer src = new KeywordTokenizer(reader);
		TokenStream tok = new LowerCaseFilter(src);
		
		return new TokenStreamComponents(src, tok);
	}
}
