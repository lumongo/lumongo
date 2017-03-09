package org.lumongo.ui.client.charting.options;

public enum StackingType {
	NONE(null),
	NORMAL("normal"),
	PERCENT("percent");

	private final String name;

	StackingType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
