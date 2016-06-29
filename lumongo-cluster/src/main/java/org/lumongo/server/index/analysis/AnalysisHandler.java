package org.lumongo.server.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.bson.Document;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AnalysisRequest;
import org.lumongo.cluster.message.Lumongo.AnalysisRequest.TermSort;
import org.lumongo.server.config.IndexConfig;
import org.lumongo.util.LumongoUtil;
import org.lumongo.util.ResultHelper;

import java.util.List;

/**
 * Created by Matt Davis on 6/29/16.
 * @author @mdavis
 */
public class AnalysisHandler {
	private final AnalysisRequest analysisRequest;
	private final String indexField;
	private final String storedFieldName;
	private final Analyzer perFieldAnalyzer;

	private final boolean docLevelEnabled;
	private final boolean summaryLevelEnabled;
	private final boolean enabled;

	private DocFreq docFreq;
	private TermFreq summaryTermFreq;

	public AnalysisHandler(IndexReader indexReader, PerFieldAnalyzerWrapper perFieldAnalyzer, IndexConfig indexConfig, AnalysisRequest analysisRequest) {
		this.analysisRequest = analysisRequest;
		this.indexField = analysisRequest.getField();
		this.storedFieldName = indexConfig.getStoredFieldName(indexField);
		this.perFieldAnalyzer = perFieldAnalyzer;

		this.docLevelEnabled = analysisRequest.getShowDocTerms() || analysisRequest.getShowDocTokens();
		this.summaryLevelEnabled = analysisRequest.getShowSummaryTerms();
		this.enabled = docLevelEnabled || summaryLevelEnabled;

		if (analysisRequest.hasMaxSegmentFreqPercent() || analysisRequest.hasMinSegmentFreqPercent() || TermSort.TFIDF.equals(analysisRequest.getTermSort())) {
			this.docFreq = new DocFreq(analysisRequest.getField(), indexReader);
			if (analysisRequest.getShowSummaryTerms()) {
				this.summaryTermFreq = new TermFreq(docFreq);
			}

		}
	}

	public Lumongo.AnalysisResult handleDocument(Document document) {

		if (storedFieldName != null && enabled) {
			Object storeFieldValues = ResultHelper.getValueFromMongoDocument(document, storedFieldName);

			Lumongo.AnalysisResult.Builder analysisResult = Lumongo.AnalysisResult.newBuilder();
			analysisResult.setField(storedFieldName);

			TermFreq docTermFreq = new TermFreq(docFreq);

			LumongoUtil.handleLists(storeFieldValues, (value) -> {
				String content = value.toString();
				try (TokenStream tokenStream = perFieldAnalyzer.tokenStream(indexField, content)) {
					tokenStream.reset();
					while (tokenStream.incrementToken()) {
						String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
						if (docLevelEnabled) {
							docTermFreq.addTerm(token);
						}
						if (summaryLevelEnabled) {
							summaryTermFreq.addTerm(token);
						}
						if (analysisRequest.getShowDocTokens()) {
							analysisResult.addToken(token);
						}

					}
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}

			});

			if (docLevelEnabled) {
				if (analysisRequest.getShowDocTokens()) {

					if (analysisRequest.getTopN() != 0) {
						List<Lumongo.Term.Builder> termBuilderList = docTermFreq.topN(analysisRequest.getTopN());
						termBuilderList.forEach(analysisResult::addTerms);
					}
					else {
						//TODO get all terms sorted
					}
				}
				return analysisResult.build();
			}
			return null;

		}
		return null;
	}

}
