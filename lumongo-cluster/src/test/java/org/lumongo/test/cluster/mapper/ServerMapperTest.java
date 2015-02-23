package org.lumongo.test.cluster.mapper;

import org.lumongo.client.command.GetFields;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.QueryResult;
import org.lumongo.fields.Mapper;
import org.lumongo.test.cluster.ServerTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.*;
import static org.testng.AssertJUnit.*;


public class ServerMapperTest extends ServerTest {

	private Mapper<Person> mapper;

	@BeforeClass
	public void test01Start() throws Exception {
		startSuite(1);

		mapper = new Mapper<>(Person.class);
	}

	@Test
	public void test02Create() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		lumongoWorkPool.createOrUpdateIndex(mapper.createOrUpdateIndex());

	}

	@Test
	public void test03Index() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		{
			Person person = new Person();
			person.firstName = "John";
			person.lastName = "Smith";
			person.id = "001";
			person.address = "444 White House Drive";
			PhoneNumber phoneNumber = new PhoneNumber();
			phoneNumber.number = "123-333-2222";
			phoneNumber.type = "Mobile";
			PhoneNumber phoneNumber2 = new PhoneNumber();
			phoneNumber2.number = "111-111-2222";
			phoneNumber2.type = "Home";

			List<PhoneNumber> phoneNumbers = new ArrayList<>();
			phoneNumbers.add(phoneNumber);
			phoneNumbers.add(phoneNumber2);
			person.phoneNumbers = phoneNumbers;
			Store store = mapper.createStore(person);
			lumongoWorkPool.store(store);
		}

		{
			Person person = new Person();
			person.firstName = "Bob";
			person.lastName = "Roberts";
			person.id = "002";
			person.address = "111 Black River Drive";
			PhoneNumber phoneNumber = new PhoneNumber();
			phoneNumber.number = "222-222-2222";
			phoneNumber.type = "Home";

			List<PhoneNumber> phoneNumbers = new ArrayList<>();
			phoneNumbers.add(phoneNumber);
			person.phoneNumbers = phoneNumbers;
			Store store = mapper.createStore(person);
			lumongoWorkPool.store(store);
		}

	}

	@Test
	public void test04Search() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		{
			Query query = new Query("person", "firstName:Bob", 10);
			QueryResult qr = lumongoWorkPool.query(query);
			assertEquals(1, qr.getTotalHits());
		}

		{
			Query query = new Query("person", "phoneNumbers.type:Mobile", 10);
			QueryResult qr = lumongoWorkPool.query(query);
			assertEquals(1, qr.getTotalHits());
		}

		{
			Query query = new Query("person", "phoneNumbers.type:Home", 10);
			QueryResult qr = lumongoWorkPool.query(query);
			assertEquals(2, qr.getTotalHits());
		}
	}

	@AfterClass
	public void test10Shutdown() throws Exception {
		stopSuite();
	}
}
