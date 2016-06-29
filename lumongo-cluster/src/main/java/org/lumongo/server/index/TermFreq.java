package org.lumongo.server.index;

import com.google.common.collect.Ordering;
import org.lumongo.cluster.message.Lumongo;

import java.io.IOException;
import java.util.ArrayList;
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

	public void makeFinal() {

	}

	public List<Lumongo.Term.Builder> topN(int n) {

		if (terms == null) {
			terms = new ArrayList<>(tokenCount.values());
		}

		if (docFreq != null) {
			for (Lumongo.Term.Builder term : terms) {
				double score = docFreq.getScoreForTerm(term.getTermFreq(), term.getDocFreq());
				term.setScore(score);
			}
		}

		//seems to be the most efficient according to
		//http://www.michaelpollmeier.com/selecting-top-k-items-from-a-list-efficiently-in-java-groovy/
		return Ordering.from((Lumongo.Term.Builder o1, Lumongo.Term.Builder o2) -> {
			if (docFreq == null) {
				return Long.compare(o1.getTermFreq(), o2.getTermFreq());
			}
			else {
				return Double.compare(o1.getScore(), o2.getScore());
			}
		}).greatestOf(terms, n);

	}
}
