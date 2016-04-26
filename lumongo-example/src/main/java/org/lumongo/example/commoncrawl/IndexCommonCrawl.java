package org.lumongo.example.commoncrawl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecord;
import org.jwat.common.Payload;
import org.lumongo.DefaultAnalyzers;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.FieldConfig.FieldType;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.FieldConfigBuilder;
import org.lumongo.util.LogUtil;
import org.lumongo.util.properties.PropertiesReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class IndexCommonCrawl {
	//fields
	private static final String UID = "uid";
	private static final String URL = "url";
	private static final String CONTENTS = "contents";
	private static final String TEXT_CONTENTS = "textContents";
	private static final String TITLE = "title";
	
	private final static Logger log = Logger.getLogger(IndexCommonCrawl.class);
	
	private static final AtomicLong count = new AtomicLong();
	private static LumongoWorkPool lumongoWorkPool;
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.err.println("usage: awsPropertiesFile prefix lumongoServers indexName");
			System.err.println("usage: aws.properties 2010/09/25/9 10.0.0.1,10.0.0.2 ccrawl");
			System.exit(1);
		}
		
		LogUtil.loadLogConfig();
		
		String propFileName = args[0];
		String prefix = args[1];
		final String[] serverNames = args[2].split(",");
		final String indexName = args[3];
		
		final LumongoPoolConfig clientConfig = new LumongoPoolConfig();
		for (String serverName : serverNames) {
			clientConfig.addMember(serverName);
		}
		
		File propFile = new File(propFileName);
		
		PropertiesReader pr = new PropertiesReader(propFile);
		
		String awsAccessKey = pr.getString("awsAccessKey");
		String awsSecretKey = pr.getString("awsSecretKey");
		
		final AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
		
		RestS3Service s3Service = new RestS3Service(awsCredentials);
		s3Service.setRequesterPaysEnabled(true);
		
		System.out.println("Fetching files list for prefix <" + prefix + ">");
		System.out.println("This can take awhile ...");
		
		S3Object[] objects = s3Service.listObjects("aws-publicdatasets", "common-crawl/crawl-002/" + prefix, null);
		System.out.println("Fetched info for <" + objects.length + "> files");
		
		lumongoWorkPool = new LumongoWorkPool(clientConfig);
		
		IndexConfig indexConfig = new IndexConfig(CONTENTS);
		indexConfig.addFieldConfig(FieldConfigBuilder.create(URL, FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create(TEXT_CONTENTS, FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create(TITLE, FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD));
		
		CreateOrUpdateIndex createOrUpdateIndex = new CreateOrUpdateIndex(indexName, 16, indexConfig);
		
		lumongoWorkPool.createOrUpdateIndex(createOrUpdateIndex);
		
		ExecutorService pool = Executors.newFixedThreadPool(16);
		
		for (S3Object object : objects) {
			final String key = object.getKey();
			
			pool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						handleFile(indexName, awsCredentials, key);
					}
					catch (Exception e) {
						log.error(e.getClass().getSimpleName() + ": ", e);
					}
				}
			});
			
		}
		
		pool.shutdown();
		lumongoWorkPool.shutdown();
		
		while (!pool.isTerminated()) {
			pool.awaitTermination(1, TimeUnit.MINUTES);
		}
		
	}
	
	private static void handleFile(String indexName, AWSCredentials awsCredentials, String key) throws S3ServiceException, IOException, ServiceException {
		
		ArcReader ar = null;
		
		try {
			
			RestS3Service s3Service = new RestS3Service(awsCredentials);
			s3Service.setRequesterPaysEnabled(true);
			
			S3Object object = s3Service.getObject("aws-publicdatasets", key);
			ar = ArcReaderFactory.getReader(object.getDataInputStream(), 1024 * 16);
			
			log.info("Opened <" + key + ">");
			
			ar.getVersionBlock();
			ArcRecord arcRecord = null;
			while ((arcRecord = ar.getNextRecord()) != null) {
				try {
					String uniqueId = arcRecord.getUrl().toString();
					
					String url = null;
					if (arcRecord.getUrl() != null) {
						url = arcRecord.getUrl().toString();
					}
					String contentType = arcRecord.getContentType();
					
					if ("text/html".equals(contentType)) {
						Payload p = arcRecord.getPayload();
						byte[] bytes = getBytes(p.getInputStream());
						
						Store s = new Store(uniqueId, indexName);
						
						try (Scanner scanner = new Scanner(new ByteArrayInputStream(bytes))) {
							String content = scanner.useDelimiter("\\A").next();
							
							Document d = Jsoup.parse(content);
							
							String pageText = d.text();
							
							String title = null;
							
							try {
								Elements e = d.head().getElementsByTag(TITLE);
								if (!e.isEmpty()) {
									title = e.get(0).text();
								}
							}
							catch (Exception e) {
								
							}
							
							if (url != null) {
								
								DBObject document = new BasicDBObject();
								document.put(CONTENTS, bytes);
								document.put(TEXT_CONTENTS, pageText);
								document.put(TITLE, title);
								document.put(URL, url);
								
								ResultDocBuilder rdBuilder = new ResultDocBuilder().setDocument(document);
								s.setResultDocument(rdBuilder);
								lumongoWorkPool.store(s);
							}
						}
						
						long c = count.getAndIncrement();
						if (c % 5000 == 0) {
							log.info("Indexed <" + c + ">");
						}
					}
					
				}
				catch (Exception e) {
					log.warn(e.getClass().getSimpleName() + ": " + e);
				}
			}
		}
		finally {
			if (ar != null) {
				log.info("Closed <" + key + ">");
				ar.close();
			}
		}
		
	}
	
	protected static byte[] getBytes(InputStream is) throws IOException {
		try {
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			
			int nRead;
			byte[] data = new byte[1024 * 16];
			
			while ((nRead = is.read(data, 0, data.length)) != -1) {
				buffer.write(data, 0, nRead);
			}
			
			buffer.flush();
			
			return buffer.toByteArray();
		}
		finally {
			if (is != null) {
				is.close();
			}
		}
	}
	
}
