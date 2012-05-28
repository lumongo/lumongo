package org.lumongo.util.properties;

import java.util.HashMap;

public class FakePropertiesReader extends PropertiesReader {
	
	private HashMap<String, String> propetiesMap;
	private String name;
	
	public FakePropertiesReader(String name, HashMap<String, String> propetiesMap) {
		super(name);
		this.propetiesMap = propetiesMap;
	}
	
	@Override
	public String getString(String key) throws PropertyException {
		String value = propetiesMap.get(key);
		if (value != null) {
			return value;
		}
		throw new PropertyException(this.name, key, "Failed to find key.");
	}
	
	@Override
	public boolean hasKey(String key) {
		return propetiesMap.containsKey(key);
	}
}
