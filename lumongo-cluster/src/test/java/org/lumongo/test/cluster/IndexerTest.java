package org.lumongo.test.cluster;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.lumongo.server.index.LumongoSegment;
import org.lumongo.util.ResultHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexerTest {

	@Test
	public void testFieldExtraction() throws Exception {

		Document testMongoDocument = new Document();
		testMongoDocument.put("field1", "someVal");
		testMongoDocument.put("myfield", 40);

		Document embeddedDocumentOne = new Document();
		embeddedDocumentOne.put("subfield1", "val2");

		Document embeddedDocumentTwo = new Document();
		embeddedDocumentTwo.put("otherfield", "1");
		embeddedDocumentOne.put("subfield2", embeddedDocumentTwo);

		testMongoDocument.put("field2", embeddedDocumentOne);

		List<Document> docs = new ArrayList<>();
		Document embeddedDocumentThree = new Document();
		embeddedDocumentThree.put("key1", "val1");
		embeddedDocumentThree.put("key2", "val2");
		Document embeddedDocumentFour = new Document();
		embeddedDocumentFour.put("key1", "someval");
		docs.add(embeddedDocumentThree);
		docs.add(embeddedDocumentFour);
		testMongoDocument.put("thisfield", docs);

		Assert.assertEquals(Arrays.asList("val1", "someval"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key1"));
		Assert.assertEquals(Arrays.asList("val2"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key2"));

		Assert.assertEquals("1", ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield"));
		Assert.assertEquals(null, ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield1"));
		Assert.assertEquals(null, ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield1.otherfield"));
		Assert.assertEquals(null, ResultHelper.getValueFromMongoDocument(testMongoDocument, "thing"));
		Assert.assertEquals("someVal", ResultHelper.getValueFromMongoDocument(testMongoDocument, "field1"));
		Assert.assertEquals("val2", ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield1"));
		Assert.assertEquals(40, ResultHelper.getValueFromMongoDocument(testMongoDocument, "myfield"));

	}
}
