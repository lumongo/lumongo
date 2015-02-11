package org.lumongo.test.client;

import org.junit.*;
import org.junit.runners.MethodSorters;
import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.Mapper;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleClientTest {


	@BeforeClass
	public static void init() throws Exception {

	}

	@AfterClass
	public static void cleanup() throws Exception {

	}

	@Test
	public void test1Mapper() {
		Mapper<MapperTest> mapper = new Mapper<>(MapperTest.class);
		CreateOrUpdateIndex ci = mapper.createOrUpdateIndex();

		Assert.assertEquals("test", ci.getIndexName());

		//TODO
		//Assert.assertEquals("id", ci.getUniqueIdField());

		IndexConfig ic = ci.getIndexConfig();


		Assert.assertEquals("testField2", ic.getDefaultSearchField());

		Lumongo.FieldConfig field1 = ic.getFieldConfig("testField1");
		Assert.assertEquals( Lumongo.LMAnalyzer.LC_KEYWORD, field1.getIndexAsList().get(0).getAnalyzer());
		Assert.assertEquals( "testField1Exact", field1.getIndexAsList().get(0).getIndexFieldName());
		Assert.assertEquals(Lumongo.LMAnalyzer.STANDARD, field1.getIndexAsList().get(1).getAnalyzer());
		Assert.assertEquals("testField1", field1.getIndexAsList().get(1).getIndexFieldName());

		Lumongo.FieldConfig field2 = ic.getFieldConfig("testField2");
		Assert.assertEquals(Lumongo.LMAnalyzer.STANDARD, field2.getIndexAsList().get(0).getAnalyzer());
		Assert.assertEquals("testField2", field2.getIndexAsList().get(0).getIndexFieldName());
	}
}
