package org.lumongo.test.client;

import org.lumongo.DefaultAnalyzers;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.Sorted;
import org.lumongo.fields.annotations.UniqueId;

@Settings(indexName = "test", numberOfSegments = 1)
public class SampleObject {

	@Indexed(analyzerName = DefaultAnalyzers.KEYWORD, fieldName = "testField1Exact")
	@Indexed(analyzerName = DefaultAnalyzers.STANDARD, fieldName = "testField1Text")
	@Faceted
	private String testField1;

	@DefaultSearch
	@Indexed(analyzerName = DefaultAnalyzers.STANDARD)
	private String testField2;

	@UniqueId
	private String id;


	@Indexed
	@Sorted
	private long someNumber;

}
