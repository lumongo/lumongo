package org.lumongo.filter;

import com.google.common.io.Resources;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.glassfish.grizzly.utils.Charsets;

import java.io.IOException;
import java.net.URL;

/**
 * Created by Matt Davis on 9/22/16.
 * @author mdavis
 * Matt Davis
 */
public class BritishUSFilter extends TokenFilter {

	private static final CharArrayMap<char[]> britishToUs = initializeDictHash();

	private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

	private static CharArrayMap<char[]> initializeDictHash() {
		CharArrayMap<char[]> charMap = new CharArrayMap<>(2000, false);

		try {
			URL url = Resources.getResource(BritishUSFilter.class, "british.txt");
			String text = Resources.toString(url, Charsets.UTF8_CHARSET);
			String[] lines = text.split("\n");
			for (String line : lines) {
				if (!line.startsWith("UK\tUS")) {
					String[] parts = line.split("\t");
					if (parts.length == 2) {
						charMap.put(parts[0].toCharArray(), parts[1].toCharArray());
					}
				}
			}

		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return charMap;

	}

	public BritishUSFilter(TokenStream in) {
		super(in);
	}

	@Override
	public final boolean incrementToken() throws IOException {
		if (!input.incrementToken()) {
			return false;
		}

		char[] replacement = britishToUs.get(termAtt.buffer(), 0, termAtt.length());
		if (replacement != null) {
			termAtt.copyBuffer(replacement, 0, replacement.length);
		}
		return true;
	}

}
