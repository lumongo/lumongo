package org.lumongo.ui.client.charting.options;

public enum ZoomType {
	NONE(""),
	X("x"),
	Y("y"),
	XY("xy");

	private final String name;

	ZoomType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
