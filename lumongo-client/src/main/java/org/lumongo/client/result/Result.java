package org.lumongo.client.result;

public class Result {

	private long commandTimeMs;

	public Result() {

	}

	public Result setCommandTimeMs(long commandTimeMs) {
		this.commandTimeMs = commandTimeMs;
		return this;
	}

	public long getCommandTimeMs() {
		return commandTimeMs;
	}
}
