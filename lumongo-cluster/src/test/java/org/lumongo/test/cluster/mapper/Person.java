package org.lumongo.test.cluster.mapper;

import org.lumongo.DefaultAnalyzers;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Embedded;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;

import java.util.List;

@Settings(indexName = "person", numberOfSegments = 1)
public class Person {

	@UniqueId
	protected String id;

	@Indexed(analyzerName = DefaultAnalyzers.LC_KEYWORD, fieldName = "firstNameExact")
	@Indexed(analyzerName = DefaultAnalyzers.STANDARD, fieldName = "firstName")
	@Faceted
	protected String firstName;

	@Indexed(analyzerName = DefaultAnalyzers.LC_KEYWORD, fieldName = "lastNameExact")
	@Indexed(analyzerName = DefaultAnalyzers.STANDARD, fieldName = "lastName")
	@Faceted
	protected String lastName;

	@DefaultSearch
	@Indexed(analyzerName = DefaultAnalyzers.STANDARD)
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
