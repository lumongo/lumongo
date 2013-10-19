package org.lumongo.example.medline;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormatSymbols;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.StoreResult;
import org.lumongo.example.medline.schema.Article;
import org.lumongo.example.medline.schema.Author;
import org.lumongo.example.medline.schema.AuthorList;
import org.lumongo.example.medline.schema.CollectiveName;
import org.lumongo.example.medline.schema.Day;
import org.lumongo.example.medline.schema.ForeName;
import org.lumongo.example.medline.schema.Initials;
import org.lumongo.example.medline.schema.Journal;
import org.lumongo.example.medline.schema.JournalIssue;
import org.lumongo.example.medline.schema.LastName;
import org.lumongo.example.medline.schema.MedlineCitation;
import org.lumongo.example.medline.schema.MedlineJournalInfo;
import org.lumongo.example.medline.schema.Month;
import org.lumongo.example.medline.schema.PubDate;
import org.lumongo.example.medline.schema.Suffix;
import org.lumongo.example.medline.schema.Year;
import org.lumongo.fields.Mapper;
import org.lumongo.util.LogUtil;
import org.lumongo.xml.StaxJAXBReader;

public class JAXBMedline {
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(JAXBMedline.class);
	
	private static LumongoWorkPool lumongoWorkPool;
	private static Mapper<Document> mapper;
	
	private static List<String> shortMonths = Arrays.asList(new DateFormatSymbols().getShortMonths());
	
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
		
		mapper = new Mapper<Document>(Document.class);
		
		@SuppressWarnings("unused")
		CreateOrUpdateIndexResult createOrUpdateResult = lumongoWorkPool.createOrUpdateIndex(mapper.createOrUpdateIndex());
		
		StaxJAXBReader<MedlineCitation> s = new StaxJAXBReader<MedlineCitation>(MedlineCitation.class, "MedlineCitation") {
			
			private int counter = 0;
			private long start = System.currentTimeMillis();
			private long last = System.currentTimeMillis();
			
			@Override
			public void handle(MedlineCitation item) throws Exception {
				final Document document = handleRecord(item);
				if (document != null) {
					
					Store store = mapper.createStore(document);
					
					@SuppressWarnings("unused")
					Future<StoreResult> sr = lumongoWorkPool.storeAsync(store);
					
				}
				if (++counter % 50000 == 0) {
					long end = System.currentTimeMillis();
					long timeForSet = end - last;
					long timeSinceStart = end - start;
					System.out.println(counter + "\t" + timeForSet + "\t" + timeSinceStart);
					last = end;
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
	
	private static Document handleRecord(final MedlineCitation medlineCitation) throws Exception {
		Document document = new Document();
		
		Article article = medlineCitation.getArticle();
		AuthorList authorList = article.getAuthorList();
		if (authorList != null) {
			for (Author author : authorList.getAuthor()) {
				String valid = author.getValidYN();
				if (valid != null && valid.equals("Y")) {
					List<Object> names = author.getLastNameOrForeNameOrInitialsOrSuffixOrCollectiveName();
					
					String firstName = null;
					@SuppressWarnings("unused")
					String initials = null;
					String lastName = null;
					String suffix = null;
					String collectiveName = null;
					
					for (Object name : names) {
						if (name instanceof LastName) {
							LastName ln = (LastName) name;
							lastName = ln.getvalue();
						}
						else if (name instanceof ForeName) {
							ForeName fn = (ForeName) name;
							firstName = fn.getvalue();
						}
						else if (name instanceof Initials) {
							Initials it = (Initials) name;
							initials = it.getvalue();
						}
						else if (name instanceof Suffix) {
							Suffix sf = (Suffix) name;
							suffix = sf.getvalue();
						}
						else if (name instanceof CollectiveName) {
							CollectiveName cn = (CollectiveName) name;
							collectiveName = cn.getvalue();
						}
						
					}
					
					if (collectiveName != null) {
						document.addAuthor(collectiveName);
					}
					if (firstName != null && lastName != null && suffix != null) {
						String authorName = firstName + " " + lastName + " " + suffix;
						document.addAuthor(authorName);
					}
					if (firstName != null && lastName != null) {
						String authorName = firstName + " " + lastName;
						document.addAuthor(authorName);
					}
					
				}
			}
		}
		
		String pmid = medlineCitation.getPMID().getvalue();
		
		document.setPmid(pmid);
		
		Journal journal = article.getJournal();
		String journalTitle = journal.getTitle();
		JournalIssue journalIssue = journal.getJournalIssue();
		
		String title = medlineCitation.getArticle().getArticleTitle();
		
		MedlineJournalInfo medlineJournalInfo = medlineCitation.getMedlineJournalInfo();
		if (medlineJournalInfo != null) {
			String issn = medlineJournalInfo.getISSNLinking();
			if (issn != null) {
				document.setIssn(issn);
			}
			
			String country = medlineJournalInfo.getCountry();
			if (country != null && !country.isEmpty()) {
				document.setJournalCountry(country);
			}
		}
		
		if (journalIssue != null) {
			String issue = journalIssue.getIssue();
			if (issue != null) {
				document.setJournalIssue(issue);
			}
			String volume = journalIssue.getVolume();
			if (volume != null) {
				document.setJournalVolume(volume);
			}
			
			PubDate pubDate = journalIssue.getPubDate();
			if (pubDate != null) {
				String year = null;
				String month = null;
				String day = null;
				// String season = null;
				// String medlineDate = null;
				
				for (Object o : pubDate.getYearOrMonthOrDayOrSeasonOrMedlineDate()) {
					if (o instanceof Year) {
						Year y = (Year) o;
						year = y.getvalue();
					}
					else if (o instanceof Month) {
						Month m = (Month) o;
						month = m.getvalue();
					}
					else if (o instanceof Day) {
						Day d = (Day) o;
						day = d.getvalue();
					}
					
					// else if (o instanceof Season) {
					// Season s = (Season) o;
					// season = s.getvalue();
					// }
					// else if (o instanceof MedlineDate) {
					// MedlineDate m = (MedlineDate) o;
					// medlineDate = m.getvalue();
					// }
				}
				Date d = null;
				if (year != null) {
					DateTime dateTime = new DateTime().withYear(Integer.parseInt(year));
					
					if (month != null) {
						dateTime = dateTime.withMonthOfYear(shortMonths.indexOf(month) + 1);
					}
					else {
						dateTime = dateTime.withMonthOfYear(1);
					}
					
					if (day != null) {
						dateTime = dateTime.withDayOfMonth(Integer.parseInt(day));
					}
					else {
						dateTime = dateTime.withDayOfMonth(1);
					}
					
					d = dateTime.toDate();
				}
				
				if (d != null) {
					document.setPublicationDate(d);
				}
				
			}
		}
		
		if (title != null) {
			document.setTitle(title);
		}
		if (journalTitle != null) {
			document.setJournalTitle(journalTitle);
		}
		
		return document;
		
	}
	
}
