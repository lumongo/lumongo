package org.lumongo.test.client;

import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.lumongo.DefaultAnalyzers;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.LumongoIndex;
import org.lumongo.cluster.message.LumongoIndex.FieldConfig;
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
	public void test1Mapper() throws Exception {
		Mapper<Person> mapper = new Mapper<>(Person.class);
		CreateOrUpdateIndex ci = mapper.createOrUpdateIndex();

		Assert.assertEquals("person", ci.getIndexName());

		//TODO
		//Assert.assertEquals("id", ci.getUniqueIdField());

		IndexConfig ic = ci.getIndexConfig();

		Assert.assertEquals("address", ic.getDefaultSearchField());

		FieldConfig field1 = ic.getFieldConfig("firstName");
		Assert.assertEquals(DefaultAnalyzers.LC_KEYWORD, field1.getIndexAsList().get(0).getAnalyzerName());
		Assert.assertEquals("firstNameExact", field1.getIndexAsList().get(0).getIndexFieldName());
		Assert.assertEquals(DefaultAnalyzers.STANDARD, field1.getIndexAsList().get(1).getAnalyzerName());
		Assert.assertEquals("firstName", field1.getIndexAsList().get(1).getIndexFieldName());

		FieldConfig field2 = ic.getFieldConfig("address");
		Assert.assertEquals(DefaultAnalyzers.STANDARD, field2.getIndexAsList().get(0).getAnalyzerName());
		Assert.assertEquals("address", field2.getIndexAsList().get(0).getIndexFieldName());

		System.out.println(ci.getIndexConfig().getIndexSettings().getFieldConfigList());

		Person to = new Person();
		to.firstName = "Matt";
		to.lastName = "Jones";
		to.address = "133 White Horse Lane";

		PhoneNumber phoneNumber = new PhoneNumber();
		phoneNumber.type = "Home";
		phoneNumber.number = "444-444-4444";
		phoneNumber.otherStuff = "This is not saved";
		to.phoneNumber = phoneNumber;

		Document obj = mapper.toDocument(to);

		Person to2 = mapper.fromDocument(obj);

	}
}
