package org.lumongo.admin;

import java.util.Arrays;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.admin.help.RequiredOptionException;
import org.lumongo.client.LumongoClient;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetIndexesResponse;
import org.lumongo.cluster.message.Lumongo.GetMembersResponse;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.IndexDeleteResponse;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;
import org.lumongo.util.LogUtil;

public class IndexAdmin {
	
	public static enum Command {
		clear,
		optimize,
		getCount,
		getCommitedCount,
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
		
		LumongoClient client = null;
		
		try {
			OptionSet options = parser.parse(args);
			
			Command command = options.valueOf(commandArg);
			String index = options.valueOf(indexArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			
			LumongoClientConfig lumongoClientConfig = new LumongoClientConfig();
			lumongoClientConfig.addMember(address, port);
			client = new LumongoClient(lumongoClientConfig);
			
			if (Command.getCount.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}
				
				GetNumberOfDocsResponse response = client.getNumberOfDocs(index, true);
				System.out.println("Segments:\n" + response.getSegmentCountResponseCount());
				System.out.println("Count:\n" + response.getNumberOfDocs());
				for (SegmentCountResponse scr : response.getSegmentCountResponseList()) {
					System.out.println("Segment " + scr.getSegmentNumber() + " Count:\n" + scr.getNumberOfDocs());
				}
			}
			if (Command.getCommitedCount.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}
				
				GetNumberOfDocsResponse response = client.getNumberOfDocs(index, false);
				System.out.println("Segments:\n" + response.getSegmentCountResponseCount());
				System.out.println("Count:\n" + response.getNumberOfDocs());
				for (SegmentCountResponse scr : response.getSegmentCountResponseList()) {
					System.out.println("Segment " + scr.getSegmentNumber() + " Count:\n" + scr.getNumberOfDocs());
				}
			}
			else if (Command.getFields.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}
				
				GetFieldNamesResponse response = client.getFieldNames(index);
				for (String fn : response.getFieldNameList()) {
					System.out.println(fn);
				}
			}
			else if (Command.optimize.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}
				
				System.out.println("Optimizing Index:\n" + index);
				@SuppressWarnings("unused")
				OptimizeResponse response = client.optimizeIndex(index);
				System.out.println("Done");
			}
			else if (Command.clear.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}
				System.out.println("Clearing Index:\n" + index);
				@SuppressWarnings("unused")
				ClearResponse response = client.clearIndex(index);
				System.out.println("Done");
			}
			else if (Command.getIndexes.equals(command)) {
				
				GetIndexesResponse response = client.getIndexes();
				
				for (String val : response.getIndexNameList()) {
					System.out.println(val);
				}
			}
			else if (Command.getCurrentMembers.equals(command)) {
				
				GetMembersResponse response = client.getCurrentMembers();
				
				System.out.println("serverAddress\thazelcastPort\tinternalPort\texternalPort");
				for (LMMember val : response.getMemberList()) {
					System.out.println(val.getServerAddress() + "\t" + val.getHazelcastPort() + "\t" + val.getInternalPort() + "\t" + val.getExternalPort());
				}
			}
			else if (Command.deleteIndex.equals(command)) {
				if (index == null) {
					throw new RequiredOptionException(AdminConstants.INDEX, command.toString());
				}
				
				System.out.println("Deleting index <" + index + ">");
				@SuppressWarnings("unused")
				IndexDeleteResponse idr = client.deleteIndex(index);
				
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
			if (client != null) {
				client.close();
			}
		}
	}
}
