package org.lumongo.server.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.lumongo.analyzer.BooleanAnalyzer;
import org.lumongo.cluster.message.Lumongo.AnalyzerSettings;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexAs;
import org.lumongo.filter.BritishUSFilter;
import org.lumongo.server.config.IndexConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.CATENATE_ALL;

public class LumongoAnalyzerFactory {
	private IndexConfig indexConfig;

	public LumongoAnalyzerFactory(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}

	public static Analyzer getPerFieldAnalyzer(AnalyzerSettings analyzerSettings) throws Exception {

		return new Analyzer() {

			@Override
			public int getPositionIncrementGap(String fieldName) {
				return 100;
			}

			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				AnalyzerSettings.Tokenizer tokenizer = analyzerSettings.getTokenizer();
				List<AnalyzerSettings.Filter> filterList = analyzerSettings.getFilterList();

				Tokenizer src;
				TokenStream tok;
				TokenStream lastTok;
				if (AnalyzerSettings.Tokenizer.KEYWORD.equals(tokenizer)) {
					src = new KeywordTokenizer();
					tok = src;
					lastTok = src;
				}
				else if (AnalyzerSettings.Tokenizer.WHITESPACE.equals(tokenizer)) {
					src = new WhitespaceTokenizer();
					tok = src;
					lastTok = src;
				}
				else if (AnalyzerSettings.Tokenizer.STANDARD.equals(tokenizer)) {
					src = new StandardTokenizer();
					tok = new StandardFilter(src);
					lastTok = tok;
				}
				else {
					throw new RuntimeException("Unknown tokenizer type <" + tokenizer);
				}

				for (AnalyzerSettings.Filter filter : filterList) {
					if (AnalyzerSettings.Filter.LOWERCASE.equals(filter)) {
						tok = new LowerCaseFilter(lastTok);
					}
					else if (AnalyzerSettings.Filter.UPPERCASE.equals(filter)) {
						tok = new UpperCaseFilter(lastTok);
					}
					else if (AnalyzerSettings.Filter.ASCII_FOLDING.equals(filter)) {
						tok = new ASCIIFoldingFilter(lastTok);
					}
					else if (AnalyzerSettings.Filter.TWO_TWO_SHINGLE.equals(filter)) {
						ShingleFilter shingleFilter = new ShingleFilter(lastTok, 2, 2);
						shingleFilter.setOutputUnigrams(false);
						tok = shingleFilter;
					}
					else if (AnalyzerSettings.Filter.THREE_THREE_SHINGLE.equals(filter)) {
						ShingleFilter shingleFilter = new ShingleFilter(lastTok, 3, 3);
						shingleFilter.setOutputUnigrams(false);
						tok = shingleFilter;
					}
					else if (AnalyzerSettings.Filter.THREE_THREE_SHINGLE.equals(filter)) {
						ShingleFilter shingleFilter = new ShingleFilter(lastTok, 3, 3);
						shingleFilter.setOutputUnigrams(false);
						tok = shingleFilter;
					}
					else if (AnalyzerSettings.Filter.THREE_THREE_SHINGLE.equals(filter)) {
						ShingleFilter shingleFilter = new ShingleFilter(lastTok, 3, 3);
						shingleFilter.setOutputUnigrams(false);
						tok = shingleFilter;
					}
					else if (AnalyzerSettings.Filter.FOUR_FOUR_SHINGLE.equals(filter)) {
						ShingleFilter shingleFilter = new ShingleFilter(lastTok, 4, 4);
						shingleFilter.setOutputUnigrams(false);
						tok = shingleFilter;
					}
					else if (AnalyzerSettings.Filter.FIVE_FIVE_SHINGLE.equals(filter)) {
						ShingleFilter shingleFilter = new ShingleFilter(lastTok, 5, 5);
						shingleFilter.setOutputUnigrams(false);
						tok = shingleFilter;
					}
					else if (AnalyzerSettings.Filter.KSTEM.equals(filter)) {
						tok = new KStemFilter(lastTok);
					}
					else if (AnalyzerSettings.Filter.STOPWORDS.equals(filter)) {
						tok = new StopFilter(lastTok, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
					}
					else if (AnalyzerSettings.Filter.ENGLISH_MIN_STEM.equals(filter)) {
						tok = new EnglishMinimalStemFilter(lastTok);
					}
					else if (AnalyzerSettings.Filter.SNOWBALL_STEM.equals(filter)) {
						tok = new SnowballFilter(lastTok, "English");
					}
					else if (AnalyzerSettings.Filter.ENGLISH_POSSESSIVE.equals(filter)) {
						tok = new EnglishPossessiveFilter(lastTok);
					}
					else if (AnalyzerSettings.Filter.MINHASH.equals(filter)) {
						tok = new MinHashFilterFactory(Collections.emptyMap()).create(lastTok);
					}
					else if (AnalyzerSettings.Filter.BRITISH_US.equals(filter)) {
						tok = new BritishUSFilter(lastTok);
					}
					else if (AnalyzerSettings.Filter.CONCAT_ALL.equals(filter)) {
						tok = new WordDelimiterGraphFilter(lastTok, CATENATE_ALL, null);
					}
					else {
						throw new RuntimeException("Unknown filter type <" + filter + ">");
					}
					lastTok = tok;
				}

				return new TokenStreamComponents(src, tok);
			}
		};

	}

	public PerFieldAnalyzerWrapper getPerFieldAnalyzer() throws Exception {
		Map<String, Analyzer> customAnalyzerMap = new HashMap<>();
		for (IndexAs indexAs : indexConfig.getIndexAsValues()) {
			String indexFieldName = indexAs.getIndexFieldName();

			FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(indexFieldName);
			AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsForIndexField(indexFieldName);

			Analyzer a;

			if (FieldConfig.FieldType.STRING.equals(fieldType)) {
				if (analyzerSettings != null) {
					a = getPerFieldAnalyzer(analyzerSettings);
				}
				else {
					a = new KeywordAnalyzer();
				}
			}
			else if (FieldConfig.FieldType.BOOL.equals(fieldType)) {
				a = new BooleanAnalyzer();
			}
			else {
				a = new KeywordAnalyzer();
			}

			customAnalyzerMap.put(indexFieldName, a);

		}

		//All fields should have analyzers defined but user queries could search against non existing field?
		Analyzer defaultAnalyzer = new KeywordAnalyzer();

		return new PerFieldAnalyzerWrapper(defaultAnalyzer, customAnalyzerMap);

	}
}
