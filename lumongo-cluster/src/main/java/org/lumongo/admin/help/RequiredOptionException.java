package org.lumongo.admin.help;

import static java.util.Collections.singletonList;
import joptsimple.OptionException;

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
