package org.lumongo.ui.client.charting.options;

public enum AlignType {
	LEFT("left"),
	CENTER("center"),
	RIGHT("right");

	private final String name;

	AlignType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
