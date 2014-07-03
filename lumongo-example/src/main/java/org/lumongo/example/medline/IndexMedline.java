package org.lumongo.example.medline;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.StoreResult;
import org.lumongo.example.medline.schema.MedlineCitation;
import org.lumongo.fields.Mapper;
import org.lumongo.util.LogUtil;
import org.lumongo.xml.StaxJAXBReader;

public class IndexMedline {
	
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(IndexMedline.class);
	
	private static LumongoWorkPool lumongoWorkPool;
	private static Mapper<MedlineDocument> mapper;
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Usage: directoryWithXml lumongoServers");
			System.out.println(" ex. /tmp/medline 10.0.0.10,10.0.0.11");
			System.out.println(" a single active lumongo server is enough, cluster membership will be updated when a connection is established");
			System.exit(1);
		}
		
		String medlineDirectory = args[0];
		String[] servers = args[1].split(",");
		
		if (!(new File(medlineDirectory)).exists()) {
			System.out.println("Directory <" + medlineDirectory + "> does not exist");
			System.exit(2);
		}
		
		LogUtil.loadLogConfig();
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.setDefaultRetries(servers.length - 1); //?
		for (String server : servers) {
			lumongoPoolConfig.addMember(server);
		}
		lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);
		lumongoWorkPool.updateMembers();
		
		mapper = new Mapper<MedlineDocument>(MedlineDocument.class);
		
		@SuppressWarnings("unused")
		CreateOrUpdateIndexResult createOrUpdateResult = lumongoWorkPool.createOrUpdateIndex(mapper.createOrUpdateIndex());
		
		final AtomicInteger counter = new AtomicInteger();
		final long start = System.currentTimeMillis();
		
		StaxJAXBReader<MedlineCitation> s = new MedlineJAXBReader(MedlineCitation.class, "MedlineCitation") {
			
			@Override
			public void handleMedlineDocument(MedlineDocument document) throws Exception {
				Store store = mapper.createStore(document);
				
				@SuppressWarnings("unused")
				Future<StoreResult> sr = lumongoWorkPool.storeAsync(store);
				
				int c = counter.incrementAndGet();
				if (c % 50000 == 0) {
					long timeSinceStart = System.currentTimeMillis() - start;
					System.out.println(timeSinceStart + "\t" + c);
				}
			}
			
		};
		
		Path medlineXmlDirectory = Paths.get(medlineDirectory);
		
		try (DirectoryStream<Path> directory = Files.newDirectoryStream(medlineXmlDirectory)) {
			for (Path file : directory) {
				System.out.println("Found <" + file.toAbsolutePath().toString() + ">");
				if (file.toAbsolutePath().toString().endsWith("xml")) {
					try {
						s.handleFile(file.toAbsolutePath().toString());
					}
					catch (Exception e) {
						System.err.println("Failed to process <" + file.toAbsolutePath().toString() + ">: " + e);
					}
				}
			}
		}
		
		lumongoWorkPool.shutdown();
	}
	
}
