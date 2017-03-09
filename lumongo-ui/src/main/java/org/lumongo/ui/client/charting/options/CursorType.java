package org.lumongo.ui.client.charting.options;

public enum CursorType {
	NORMAL(null),
	POINTER("pointer");

	private final String name;

	CursorType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
