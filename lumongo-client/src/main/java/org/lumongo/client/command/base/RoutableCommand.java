package org.lumongo.client.command.base;

public interface RoutableCommand {
	public String getUniqueId();

	public String getIndexName();
}
