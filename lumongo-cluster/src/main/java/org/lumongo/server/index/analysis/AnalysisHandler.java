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
	private final int minWordLength;
	private final int maxWordLength;

	private Integer minSegmentDocFreqCount;
	private Integer maxSegmentDocFreqCount;

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

		this.minWordLength = analysisRequest.getMinWordLength();
		this.maxWordLength = analysisRequest.getMaxWordLength();

		boolean needDocFreq = (analysisRequest.hasMinSegmentFreqPercent() || analysisRequest.hasMinSegmentFreqPercent() || analysisRequest
				.hasMinSegmentFreqCount() || analysisRequest.hasMaxSegmentFreqCount() || TermSort.TFIDF.equals(analysisRequest.getTermSort()));

		if (needDocFreq) {
			this.docFreq = new DocFreq(indexReader, analysisRequest.getField());
			if (analysisRequest.hasMinSegmentFreqPercent()) {
				this.minSegmentDocFreqCount = docFreq.getNumDocsForPercent(analysisRequest.getMinSegmentFreqPercent());
			}
			if (analysisRequest.hasMaxSegmentFreqPercent()) {
				this.maxSegmentDocFreqCount = docFreq.getNumDocsForPercent(analysisRequest.getMaxSegmentFreqPercent());
			}

			if (analysisRequest.hasMinSegmentFreqCount()) {
				this.minSegmentDocFreqCount = analysisRequest.getMinSegmentFreqCount();
			}

			if (analysisRequest.hasMaxSegmentFreqCount()) {
				this.maxSegmentDocFreqCount = analysisRequest.getMaxSegmentFreqCount();
			}

		}

		if (summaryLevelEnabled) {
			this.summaryTermFreq = new TermFreq(docFreq);
		}
	}

	public static void handleDocument(org.bson.Document doc, List<AnalysisHandler> analysisHandlerList, Lumongo.ScoredResult.Builder srBuilder) {
		for (AnalysisHandler analysisHandler : analysisHandlerList) {
			Lumongo.AnalysisResult analysisResult = analysisHandler.handleDocument(doc);
			if (analysisResult != null) {
				srBuilder.addAnalysisResult(analysisResult);
			}
		}
	}

	public Lumongo.AnalysisResult handleDocument(Document document) {

		if (storedFieldName != null && enabled) {
			Object storeFieldValues = ResultHelper.getValueFromMongoDocument(document, storedFieldName);

			Lumongo.AnalysisResult.Builder analysisResult = Lumongo.AnalysisResult.newBuilder();
			analysisResult.setField(storedFieldName);

			TermFreq docTermFreq = null;
			if (docLevelEnabled) {
				docTermFreq = new TermFreq(docFreq);
			}
			final TermFreq docTermFreqFinal = docTermFreq;

			LumongoUtil.handleLists(storeFieldValues, (value) -> {
				String content = value.toString();
				try (TokenStream tokenStream = perFieldAnalyzer.tokenStream(indexField, content)) {
					tokenStream.reset();
					while (tokenStream.incrementToken()) {
						String token = tokenStream.getAttribute(CharTermAttribute.class).toString();

						if (analysisRequest.getShowDocTokens()) {
							analysisResult.addToken(token);
						}

						if (minWordLength > 0) {
							if (token.length() < minWordLength) {
								continue;
							}
						}
						if (maxWordLength > 0) {
							if (token.length() > maxWordLength) {
								continue;
							}
						}

						if (maxSegmentDocFreqCount != null || minSegmentDocFreqCount != null) {
							int termDocFreq = this.docFreq.getDocFreq(token);

							if (minSegmentDocFreqCount != null) {
								if (termDocFreq < minSegmentDocFreqCount) {
									continue;
								}
							}
							if (maxSegmentDocFreqCount != null) {
								if (termDocFreq > maxSegmentDocFreqCount) {
									continue;
								}
							}
						}

						if (docLevelEnabled) {
							docTermFreqFinal.addTerm(token);
						}
						if (summaryLevelEnabled) {
							summaryTermFreq.addTerm(token);
						}

					}
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}

			});

			if (docLevelEnabled) {
				if (analysisRequest.getShowDocTokens()) {

					List<Lumongo.Term.Builder> termBuilderList = docTermFreq.topN(analysisRequest.getTopN(), analysisRequest.getTermSort());
					termBuilderList.forEach(analysisResult::addTerms);

				}
				return analysisResult.build();
			}
			return null;

		}
		return null;
	}

}
