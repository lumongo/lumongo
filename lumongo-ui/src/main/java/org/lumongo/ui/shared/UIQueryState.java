package org.lumongo.ui.shared;

import java.util.List;

/**
 * Created by Payam Meyer on 3/21/17.
 * @author pmeyer
 */
public class UIQueryState {

	private static List<IndexInfo> indexes;

	public static List<IndexInfo> getIndexes() {
		return indexes;
	}

	public static void setIndexes(List<IndexInfo> indexes) {
		UIQueryState.indexes = indexes;
	}
}
