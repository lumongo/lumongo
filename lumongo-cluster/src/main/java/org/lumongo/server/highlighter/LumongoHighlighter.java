package org.lumongo.server.highlighter;

import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.Scorer;
import org.lumongo.cluster.message.Lumongo;

/**
 * Created by Matt Davis on 6/21/16.
 * @author mdavis
 */
public class LumongoHighlighter extends Highlighter {
	private final Lumongo.HighlightRequest highlightRequest;

	public LumongoHighlighter(Formatter formatter, Scorer fragmentScorer, Lumongo.HighlightRequest highlightRequest) {
		super(formatter, fragmentScorer);
		this.highlightRequest = highlightRequest;
	}

	public Lumongo.HighlightRequest getHighlight() {
		return highlightRequest;
	}
}
