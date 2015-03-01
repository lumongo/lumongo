package org.lumongo.admin.help;

import joptsimple.OptionException;

import static java.util.Collections.singletonList;

public class RequiredOptionException extends OptionException {
	
	private static final long serialVersionUID = 1L;
	private String command;
	
	public RequiredOptionException(String option, String command) {
		super(singletonList(option));
		this.command = command;
	}
	
	@Override
	public String getMessage() {
		return singleOptionMessage() + " is required for " + command;
	}
}
