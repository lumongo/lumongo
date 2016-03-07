package org.lumongo.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by Matt Davis on 2/10/16.
 * Based on org.apache.solr.schema.BoolField from Solr
 */
public class BooleanAnalyzer extends Analyzer{

	protected final static char[] TRUE_TOKEN = {'T'};
	protected final static char[] FALSE_TOKEN = {'F'};


	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		Tokenizer tokenizer = new Tokenizer() {
			final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
			boolean done = false;

			@Override
			public void reset() throws IOException {
				super.reset();
				done = false;
			}

			@Override
			public boolean incrementToken() throws IOException {
				clearAttributes();
				if (done) return false;
				done = true;
				int ch = input.read();
				if (ch==-1) return false;
				termAtt.copyBuffer(
						((ch=='t' || ch=='T' || ch=='1') ? TRUE_TOKEN : FALSE_TOKEN)
						,0,1);
				return true;
			}
		};

		return new TokenStreamComponents(tokenizer);
	}
}
