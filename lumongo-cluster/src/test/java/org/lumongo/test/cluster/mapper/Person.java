package org.lumongo.test.cluster.mapper;

import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.annotations.*;

import java.util.List;

@Settings(indexName = "person", numberOfSegments = 1)
public class Person {


	@UniqueId
	protected String id;

	@Indexed(analyzer = Lumongo.LMAnalyzer.LC_KEYWORD, fieldName = "firstNameExact")
	@Indexed(analyzer = Lumongo.LMAnalyzer.STANDARD, fieldName = "firstName")
	@Faceted
	protected String firstName;


	@Indexed(analyzer = Lumongo.LMAnalyzer.LC_KEYWORD, fieldName = "lastNameExact")
	@Indexed(analyzer = Lumongo.LMAnalyzer.STANDARD, fieldName = "lastName")
	@Faceted
	protected String lastName;

	@DefaultSearch
	@Indexed(analyzer = Lumongo.LMAnalyzer.STANDARD)
	protected String address;

	@Embedded
	protected List<PhoneNumber> phoneNumbers;


	@Override
	public String toString() {
		return "Person{" +
						"id='" + id + '\'' +
						", firstName='" + firstName + '\'' +
						", lastName='" + lastName + '\'' +
						", address='" + address + '\'' +
						", phoneNumbers=" + phoneNumbers +
						'}';
	}

}
