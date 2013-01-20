package org.lumongo.admin;

import java.util.Arrays;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.admin.help.RequiredOptionException;
import org.lumongo.client.command.ClearIndex;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.GetFields;
import org.lumongo.client.command.GetIndexes;
import org.lumongo.client.command.GetMembers;
import org.lumongo.client.command.GetNumberOfDocs;
import org.lumongo.client.command.OptimizeIndex;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoBaseWorkPool;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.result.ClearIndexResult;
import org.lumongo.client.result.DeleteIndexResult;
import org.lumongo.client.result.GetFieldsResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.client.result.GetNumberOfDocsResult;
import org.lumongo.client.result.OptimizeIndexResult;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;
import org.lumongo.util.LogUtil;

public class IndexAdmin {

	public static enum Command {
		clear,
		optimize,
		getRealTimeCount,
		getCount,
		getFields,
		getIndexes,
		getCurrentMembers,
		deleteIndex
	}

	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();

		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts(AdminConstants.PORT).withRequiredArg().ofType(Integer.class).defaultsTo(32191)
				.describedAs("Lumongo external port");
		OptionSpec<String> indexArg = parser.accepts(AdminConstants.INDEX).withRequiredArg().describedAs("Index to perform action");
		OptionSpec<Command> commandArg = parser.accepts(AdminConstants.COMMAND).withRequiredArg().ofType(Command.class).required()
				.describedAs("Command to run " + Arrays.toString(Command.values()));

		LumongoBaseWorkPool lumongoWorkPool = null;

		try {
			OptionSet options = parser.parse(args);

			Command command = options.valueOf(commandArg);
			String index = options.valueOf(indexArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);

			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
			lumongoPoolConfig.addMember(address, port);
			lumongoWorkPool = new LumongoBaseWorkPool(new LumongoPool(lumongoPoolConfig));

			if (Command.getRealTimeCount.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}

				GetNumberOfDocsResult response = lumongoWorkPool.execute(new GetNumberOfDocs(index).setRealTime(true));
				System.out.println("Segments:\n" + response.getSegmentCountResponseCount());
				System.out.println("Count:\n" + response.getNumberOfDocs());
				for (SegmentCountResponse scr : response.getSegmentCountResponses()) {
					System.out.println("Segment " + scr.getSegmentNumber() + " Count:\n" + scr.getNumberOfDocs());
				}
			}
			else if (Command.getCount.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}

				GetNumberOfDocsResult response = lumongoWorkPool.execute(new GetNumberOfDocs(index).setRealTime(false));
				System.out.println("Segments:\n" + response.getSegmentCountResponseCount());
				System.out.println("Count:\n" + response.getNumberOfDocs());
				for (SegmentCountResponse scr : response.getSegmentCountResponses()) {
					System.out.println("Segment " + scr.getSegmentNumber() + " Count:\n" + scr.getNumberOfDocs());
				}
			}
			else if (Command.getFields.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}

				GetFieldsResult response = lumongoWorkPool.execute(new GetFields(index));
				for (String fn : response.getFieldNames()) {
					System.out.println(fn);
				}
			}
			else if (Command.optimize.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}

				System.out.println("Optimizing Index:\n" + index);
				@SuppressWarnings("unused")
				OptimizeIndexResult response = lumongoWorkPool.execute(new OptimizeIndex(index));
				System.out.println("Done");
			}
			else if (Command.clear.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}
				System.out.println("Clearing Index:\n" + index);
				@SuppressWarnings("unused")
				ClearIndexResult response = lumongoWorkPool.execute(new ClearIndex(index));
				System.out.println("Done");
			}
			else if (Command.getIndexes.equals(command)) {

				GetIndexesResult response = lumongoWorkPool.execute(new GetIndexes());
				for (String val : response.getIndexNames()) {
					System.out.println(val);
				}
			}
			else if (Command.getCurrentMembers.equals(command)) {

				GetMembersResult response = lumongoWorkPool.execute(new GetMembers());

				System.out.println("serverAddress\thazelcastPort\tinternalPort\texternalPort");
				for (LMMember val : response.getMembers()) {
					System.out.println(val.getServerAddress() + "\t" + val.getHazelcastPort() + "\t" + val.getInternalPort() + "\t" + val.getExternalPort());
				}
			}
			else if (Command.deleteIndex.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}

				System.out.println("Deleting index <" + index + ">");
				@SuppressWarnings("unused")
				DeleteIndexResult response = lumongoWorkPool.execute(new DeleteIndex(index));

				System.out.println("Deleted index <" + index + ">");

			}
			else {
				System.err.println(command + " not supported");
			}

		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.err);
		}
		finally {
			if (lumongoWorkPool != null) {
				lumongoWorkPool.shutdown();
			}
		}
	}
}
