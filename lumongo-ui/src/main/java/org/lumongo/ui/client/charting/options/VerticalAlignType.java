package org.lumongo.ui.client.charting.options;

public enum VerticalAlignType {
	TOP("top"),
	MIDDLE("middle"),
	BOTTOM("bottom");

	private final String name;

	VerticalAlignType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
