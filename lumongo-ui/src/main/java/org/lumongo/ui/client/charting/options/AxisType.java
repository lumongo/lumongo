package org.lumongo.ui.client.charting.options;

public enum AxisType {
	LINEAR("linear"),
	LOGARITHMIC("logarithmic"),
	DATETIME("datetime"),
	CATEGORY("category");

	private final String name;

	AxisType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
