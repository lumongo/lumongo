package org.lumongo.server.index.analysis;

import com.google.common.collect.Ordering;
import org.lumongo.cluster.message.Lumongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 * Created by mdavis on 6/28/16.
 */
public class TermFreq {

	private final DocFreq docFreq;
	private final HashMap<String, Lumongo.Term.Builder> tokenCount;
	private List<Lumongo.Term.Builder> terms;

	public TermFreq(DocFreq docFreq) {
		this.docFreq = docFreq;
		this.tokenCount = new HashMap<>();
	}

	public void addTerm(String term) throws IOException {

		Lumongo.Term.Builder lmTerm = tokenCount.get(term);
		if (lmTerm == null) {
			lmTerm = Lumongo.Term.newBuilder().setTermFreq(0).setValue(term);
			tokenCount.put(term, lmTerm);
			if (docFreq != null) {
				int docFreq = this.docFreq.getDocFreq(term);
				lmTerm.setDocFreq(docFreq);
			}
		}

		lmTerm.setTermFreq(lmTerm.getTermFreq() + 1);
	}

	public List<Lumongo.Term.Builder> getTopTerms(int topN, Lumongo.AnalysisRequest.TermSort termSort, int docCount) {

		if (terms == null) {
			terms = new ArrayList<>(tokenCount.values());
		}

		if (Lumongo.AnalysisRequest.TermSort.TFIDF.equals(termSort)) {
			if (docFreq != null) {
				for (Lumongo.Term.Builder term : terms) {
					double score = docFreq.getScoreForTerm(term.getTermFreq() / docCount, term.getDocFreq());
					term.setScore(score);
				}
			}
		}

		return getTopTerms(terms, topN, termSort);

	}

	public static List<Lumongo.Term.Builder> getTopTerms(List<Lumongo.Term.Builder> terms, int n, Lumongo.AnalysisRequest.TermSort termSort) {
		Comparator<Lumongo.Term.Builder> ordering = (Lumongo.Term.Builder o1, Lumongo.Term.Builder o2) -> {
			if (Lumongo.AnalysisRequest.TermSort.TF.equals(termSort)) {
				return Long.compare(o1.getTermFreq(), o2.getTermFreq());
			}
			else if (Lumongo.AnalysisRequest.TermSort.TFIDF.equals(termSort)) {
				return Double.compare(o1.getScore(), o2.getScore());
			}
			else if (Lumongo.AnalysisRequest.TermSort.ABC.equals(termSort)) {
				return o2.getValue().compareTo(o1.getValue());
			}
			else {
				return 0;
			}
		};

		if (n != 0) {
			//seems to be the most efficient according to
			//http://www.michaelpollmeier.com/selecting-top-k-items-from-a-list-efficiently-in-java-groovy/
			return Ordering.from(ordering).greatestOf(terms, n);
		}
		else {
			Collections.sort(terms, ordering.reversed());
			return terms;
		}
	}

}
