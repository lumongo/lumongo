package org.lumongo.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.lumongo.LumongoLuceneConstants;

public class LowercaseKeywordAnalyzer extends Analyzer {
	public LowercaseKeywordAnalyzer() {
	}
	
	@Override
	protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		KeywordTokenizer src = new KeywordTokenizer(reader);
		TokenStream tok = new LowerCaseFilter(LumongoLuceneConstants.VERSION, src);
		
		return new TokenStreamComponents(src, tok);
	}
}
