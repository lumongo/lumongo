package org.lumongo.test.cluster;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Assert;
import org.junit.Test;
import org.lumongo.server.indexing.LumongoSegment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexerTest {

	@Test
	public void testFieldExtraction() throws Exception {

		DBObject testObject = new BasicDBObject();
		testObject.put("field1", "someVal");
		testObject.put("myfield", 40);

		DBObject embeddedObject1 = new BasicDBObject();
		embeddedObject1.put("subfield1", "val2");

		DBObject embeddedObject2 = new BasicDBObject();
		embeddedObject2.put("otherfield", "1");
		embeddedObject1.put("subfield2", embeddedObject2);

		testObject.put("field2", embeddedObject1);

		List<DBObject> objs = new ArrayList<>();
		DBObject embeddedObject3 = new BasicDBObject();
		embeddedObject3.put("key1", "val1");
		embeddedObject3.put("key2", "val2");
		DBObject embeddedObject4 = new BasicDBObject();
		embeddedObject4.put("key1", "someval");
		objs.add(embeddedObject3);
		objs.add(embeddedObject4);
		testObject.put("thisfield", objs);

		Assert.assertEquals(Arrays.asList("val1", "someval"), LumongoSegment.getValueFromDocument(testObject, "thisfield.key1"));
		Assert.assertEquals(Arrays.asList("val2"), LumongoSegment.getValueFromDocument(testObject, "thisfield.key2"));

		Assert.assertEquals("1", LumongoSegment.getValueFromDocument(testObject, "field2.subfield2.otherfield"));
		Assert.assertEquals(null, LumongoSegment.getValueFromDocument(testObject, "field2.subfield2.otherfield1"));
		Assert.assertEquals(null, LumongoSegment.getValueFromDocument(testObject, "field2.subfield1.otherfield"));
		Assert.assertEquals(null, LumongoSegment.getValueFromDocument(testObject, "thing"));
		Assert.assertEquals("someVal", LumongoSegment.getValueFromDocument(testObject, "field1"));
		Assert.assertEquals("val2", LumongoSegment.getValueFromDocument(testObject, "field2.subfield1"));
		Assert.assertEquals(40, LumongoSegment.getValueFromDocument(testObject, "myfield"));

	}
}
