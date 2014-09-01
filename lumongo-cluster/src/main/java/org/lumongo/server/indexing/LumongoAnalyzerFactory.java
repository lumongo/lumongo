package org.lumongo.server.indexing;

import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.lumongo.analyzer.LowercaseKeywordAnalyzer;
import org.lumongo.analyzer.LowercaseWhitespaceAnalyzer;
import org.lumongo.analyzer.StandardFoldingAnalyzer;
import org.lumongo.analyzer.StandardFoldingNoStopAnalyzer;
import org.lumongo.analyzer.StandardNoStopAnalyzer;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.server.config.IndexConfig;

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
		
		throw new Exception("Unsupport analyzer <" + lmAnalyzer + ">");
		
	}
	
	private IndexConfig indexConfig;
	
	public LumongoAnalyzerFactory(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}
	
	public Analyzer getAnalyzer() throws Exception {
		HashMap<String, Analyzer> customAnalyzerMap = new HashMap<String, Analyzer>();
		for (FieldConfig fieldConfig : indexConfig.getFieldConfigList()) {
			Analyzer a = getAnalyzer(fieldConfig.getAnalyzer());
			customAnalyzerMap.put(fieldConfig.getFieldName(), a);
			
		}
		
		Analyzer defaultAnalyzer = getAnalyzer(indexConfig.getDefaultAnalyzer());
		
		PerFieldAnalyzerWrapper aWrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer, customAnalyzerMap);
		return aWrapper;
	}
	
}
