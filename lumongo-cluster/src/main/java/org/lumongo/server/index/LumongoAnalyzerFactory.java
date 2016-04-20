package org.lumongo.server.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.lsh.MinHashFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.lumongo.analyzer.BooleanAnalyzer;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.IndexAs;
import org.lumongo.server.config.IndexConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LumongoAnalyzerFactory {
	public static Analyzer getStringAnalyzer(Lumongo.AnalyzerSettings analyzerSettings) throws Exception {

		return new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				Lumongo.AnalyzerSettings.Tokenizer tokenizer = analyzerSettings.getTokenizer();
				List<Lumongo.AnalyzerSettings.Filter> filterList = analyzerSettings.getFilterList();

				Tokenizer src;
				TokenStream tok;
				TokenStream lastTok;
				if (Lumongo.AnalyzerSettings.Tokenizer.KEYWORD.equals(tokenizer)) {
					src = new KeywordTokenizer();
					tok = src;
					lastTok = src;
				}
				else if (Lumongo.AnalyzerSettings.Tokenizer.WHITESPACE.equals(tokenizer)) {
					src = new WhitespaceTokenizer();
					tok = src;
					lastTok = src;
				}
				else if (Lumongo.AnalyzerSettings.Tokenizer.STANDARD.equals(tokenizer)) {
					src = new StandardTokenizer();
					tok = new StandardFilter(src);
					lastTok = tok;
				}
				else {
					throw new RuntimeException("Unknown tokenizer type <" + tokenizer);
				}

				for (Lumongo.AnalyzerSettings.Filter filter : filterList) {
					if (Lumongo.AnalyzerSettings.Filter.LOWERCASE.equals(filter)) {
						tok = new LowerCaseFilter(lastTok);
					}
					else if (Lumongo.AnalyzerSettings.Filter.UPPERCASE.equals(filter)) {
						tok = new UpperCaseFilter(lastTok);
					}
					else if (Lumongo.AnalyzerSettings.Filter.ASCII_FOLDING.equals(filter)) {
						tok = new ASCIIFoldingFilter(lastTok);
					}
					else if (Lumongo.AnalyzerSettings.Filter.KSTEM.equals(filter)) {
						tok = new KStemFilter(lastTok);
					}
					else if (Lumongo.AnalyzerSettings.Filter.STOPWORDS.equals(filter)) {
						tok = new StopFilter(lastTok, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
					}
					else if (Lumongo.AnalyzerSettings.Filter.ENGLISH_MIN_STEM.equals(filter)) {
						tok = new EnglishMinimalStemFilter(lastTok);
					}
					else if (Lumongo.AnalyzerSettings.Filter.SNOWBALL_STEM.equals(filter)) {
						tok = new SnowballFilter(lastTok, "English");
					}
					else if (Lumongo.AnalyzerSettings.Filter.ENGLISH_POSSESSIVE.equals(filter)) {
						tok = new EnglishPossessiveFilter(lastTok);
					}
					else if (Lumongo.AnalyzerSettings.Filter.MINHASH.equals(filter)) {
						tok = new MinHashFilter(lastTok, 100);
					}
					else {
						throw new RuntimeException("Unknown filter type <" + filter + ">");
					}
				}


				return new TokenStreamComponents(src, tok);
			}
		} ;


		
	}
	
	private IndexConfig indexConfig;
	
	public LumongoAnalyzerFactory(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}
	
	public Analyzer getStringAnalyzer() throws Exception {
		Map<String, Analyzer> customAnalyzerMap = new HashMap<>();
		for (IndexAs indexAs : indexConfig.getIndexAsValues()) {

			Analyzer a;
			if (IndexAs.FieldType.STRING.equals(indexAs.getFieldType())) {
				Lumongo.AnalyzerSettings analyzerSetting = indexAs.getAnalyzerSetting();
				if (analyzerSetting != null) {
					a = getStringAnalyzer(analyzerSetting);
				}
				else {
					a = new KeywordAnalyzer();
				}
			}
			else if (IndexAs.FieldType.BOOL.equals(indexAs.getFieldType())) {
				a = new BooleanAnalyzer();
			}
			else {
				a = new KeywordAnalyzer();
			}

			customAnalyzerMap.put(indexAs.getIndexFieldName(), a);
			
		}
		
		//All fields should have analyzers defined but user queries could search against non existing field?
		Analyzer defaultAnalyzer = new KeywordAnalyzer();

		return new PerFieldAnalyzerWrapper(defaultAnalyzer, customAnalyzerMap);
	}
	
}
