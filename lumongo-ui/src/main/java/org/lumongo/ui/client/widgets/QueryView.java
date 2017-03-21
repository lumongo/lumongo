package org.lumongo.ui.client.widgets;

import com.google.common.base.Joiner;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Payam Meyer on 3/21/17.
 * @author pmeyer
 */
public class QueryView {

	private static final Joiner orJoiner = Joiner.on(" OR ");
	private static final Joiner andJoiner = Joiner.on(" AND ");

	private String addOrQueryFilter(Collection<String> values) {
		if (values != null && !values.isEmpty()) {

			List<String> quotedValues = values.stream().map(value -> "\"" + value + "\"").collect(Collectors.toList());
			return orJoiner.join(quotedValues);

		}

		return "";

	}

	private String addAndQueryFilter(Collection<String> values) {
		if (values != null && !values.isEmpty()) {

			List<String> quotedValues = values.stream().map(value -> "\"" + value + "\"").collect(Collectors.toList());
			return andJoiner.join(quotedValues);

		}

		return "";

	}

}
