package org.lumongo.test.client;

import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Embedded;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;

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
	protected PhoneNumber phoneNumber;

	@Override
	public String toString() {
		return "Person{" +
						"id='" + id + '\'' +
						", firstName='" + firstName + '\'' +
						", lastName='" + lastName + '\'' +
						", address='" + address + '\'' +
						", phoneNumber=" + phoneNumber +
						'}';
	}

}
