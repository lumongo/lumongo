package org.lumongo.ui.client.charting.options;

public enum AxisAlignType {
	LOW("left"),
	MIDDLE("center"),
	HIGH("right");

	private final String name;

	AxisAlignType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
