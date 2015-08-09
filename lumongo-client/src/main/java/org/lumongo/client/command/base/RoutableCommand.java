package org.lumongo.client.command.base;

public interface RoutableCommand {
	String getUniqueId();

	String getIndexName();
}
