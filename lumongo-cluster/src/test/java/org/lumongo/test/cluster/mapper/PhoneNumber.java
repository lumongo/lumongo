package org.lumongo.test.cluster.mapper;

import org.lumongo.DefaultAnalyzers;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.NotSaved;

public class PhoneNumber {

	@Indexed(analyzerName = DefaultAnalyzers.LC_KEYWORD)
	@Faceted
	protected String type;

	@Indexed(analyzerName = DefaultAnalyzers.STANDARD)
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

