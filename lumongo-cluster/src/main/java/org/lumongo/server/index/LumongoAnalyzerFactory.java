package org.lumongo.server.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.lsh.LSHAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.lumongo.analyzer.BooleanAnalyzer;
import org.lumongo.analyzer.LowercaseKeywordAnalyzer;
import org.lumongo.analyzer.LowercaseWhitespaceAnalyzer;
import org.lumongo.analyzer.StandardFoldingAnalyzer;
import org.lumongo.analyzer.StandardFoldingNoStopAnalyzer;
import org.lumongo.analyzer.StandardNoStopAnalyzer;
import org.lumongo.cluster.message.Lumongo.IndexAs;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.server.config.IndexConfig;

import java.util.HashMap;
import java.util.Map;

public class LumongoAnalyzerFactory {
	public static Analyzer getAnalyzer(LMAnalyzer lmAnalyzer) throws Exception {
		if (LMAnalyzer.KEYWORD.equals(lmAnalyzer)) {
			return new KeywordAnalyzer();
		}
		else if (LMAnalyzer.LC_KEYWORD.equals(lmAnalyzer)) {
			return new LowercaseKeywordAnalyzer();
		}
		else if (LMAnalyzer.WHITESPACE.equals(lmAnalyzer)) {
			return new WhitespaceAnalyzer();
		}
		else if (LMAnalyzer.LC_WHITESPACE.equals(lmAnalyzer)) {
			return new LowercaseWhitespaceAnalyzer();
		}
		else if (LMAnalyzer.STANDARD.equals(lmAnalyzer)) {
			return new StandardAnalyzer();
		}
		else if (LMAnalyzer.STANDARD_FOLDING.equals(lmAnalyzer)) {
			return new StandardFoldingAnalyzer();
		}
		else if (LMAnalyzer.STANDARD_NO_STOP.equals(lmAnalyzer)) {
			return new StandardNoStopAnalyzer();
		}
		else if (LMAnalyzer.STANDARD_FOLDING_NO_STOP.equals(lmAnalyzer)) {
			return new StandardFoldingNoStopAnalyzer();
		}
		else if (LMAnalyzer.NUMERIC_INT.equals(lmAnalyzer)) {
			return new KeywordAnalyzer();
		}
		else if (LMAnalyzer.NUMERIC_LONG.equals(lmAnalyzer)) {
			return new KeywordAnalyzer();
		}
		else if (LMAnalyzer.NUMERIC_FLOAT.equals(lmAnalyzer)) {
			return new KeywordAnalyzer();
		}
		else if (LMAnalyzer.NUMERIC_DOUBLE.equals(lmAnalyzer)) {
			return new KeywordAnalyzer();
		}
		else if (LMAnalyzer.DATE.equals(lmAnalyzer)) {
			return new KeywordAnalyzer();
		}
		else if (LMAnalyzer.LSH.equals(lmAnalyzer)) {
			return new LSHAnalyzer(100);
		}
		else if (LMAnalyzer.BOOL.equals(lmAnalyzer)) {
			return new BooleanAnalyzer();
		}
		
		throw new Exception("Unsupported analyzer <" + lmAnalyzer + ">");
		
	}
	
	private IndexConfig indexConfig;
	
	public LumongoAnalyzerFactory(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}
	
	public Analyzer getAnalyzer() throws Exception {
		Map<String, Analyzer> customAnalyzerMap = new HashMap<>();
		for (IndexAs indexAs : indexConfig.getIndexAsValues()) {
			Analyzer a = getAnalyzer(indexAs.getAnalyzer());
			customAnalyzerMap.put(indexAs.getIndexFieldName(), a);
			
		}
		
		//All fields should have analyzers defined but user queries could search against non existing field?
		Analyzer defaultAnalyzer = new KeywordAnalyzer();

		return new PerFieldAnalyzerWrapper(defaultAnalyzer, customAnalyzerMap);
	}
	
}
