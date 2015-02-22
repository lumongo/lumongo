package org.lumongo.test.cluster.mapper;

import org.lumongo.client.command.Store;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.fields.Mapper;
import org.lumongo.test.cluster.ServerTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MapperTest extends ServerTest {

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
			person.phoneNumber = phoneNumber;
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
			person.phoneNumber = phoneNumber;
			Store store = mapper.createStore(person);
			lumongoWorkPool.store(store);
		}

	}

	@AfterClass
	public void test10Shutdown() throws Exception {
		stopSuite();
	}
}
