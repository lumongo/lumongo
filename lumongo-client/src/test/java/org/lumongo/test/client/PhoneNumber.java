package org.lumongo.test.client;

import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.NotSaved;

public class PhoneNumber {

	@Indexed(analyzer = Lumongo.LMAnalyzer.LC_KEYWORD)
	@Faceted
	protected String type;

	@Indexed(analyzer = Lumongo.LMAnalyzer.STANDARD)
	protected String number;

	@NotSaved
	protected String otherStuff;

	@Override
	public String toString() {
		return "PhoneNumber{" +
						"type='" + type + '\'' +
						", number='" + number + '\'' +
						", otherStuff='" + otherStuff + '\'' +
						'}';
	}
}

