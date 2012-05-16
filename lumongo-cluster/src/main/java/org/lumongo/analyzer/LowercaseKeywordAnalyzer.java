package org.lumongo.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.lumongo.LuceneConstants;

public class LowercaseKeywordAnalyzer extends ReusableAnalyzerBase {
	
	public LowercaseKeywordAnalyzer() {
		
	}
	
	@Override
	protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		
		KeywordTokenizer src = new KeywordTokenizer(reader);
		TokenStream tok = new LowerCaseFilter(LuceneConstants.VERSION, src);
		
		return new TokenStreamComponents(src, tok);
	}
	
}
