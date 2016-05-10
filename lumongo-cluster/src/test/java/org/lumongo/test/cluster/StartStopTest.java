package org.lumongo.test.cluster;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.lumongo.DefaultAnalyzers;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.FacetAs.DateHandling;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FieldConfig.FieldType;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.FieldConfigBuilder;
import org.lumongo.storage.lucene.MongoFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;

public class StartStopTest extends ServerTestBase {

	public static final String FACET_TEST_INDEX = "plugged-54a725bc148f6dd7d62bc600";
	//public static final String FACET_TEST_INDEX = "plugged";

	private final int COUNT_PER_ISSN = 10;
	private final String uniqueIdPrefix = "myId-";

	private final String[] issns = new String[] { "1234-1234", "3333-1234", "1234-5555", "1234-4444", "2222-2222" };
	private int totalRecords = COUNT_PER_ISSN * issns.length;

	@BeforeClass
	public void test01Start() throws Exception {
		startSuite(1);
	}

	@Test
	public void test02Init() throws Exception {

		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		String defaultSearchField = "title";
		IndexConfig indexConfig = new IndexConfig(defaultSearchField);

		indexConfig.setSegmentTolerance(0.05);
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("eissn", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("uid", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("an", FieldType.NUMERIC_INT).index());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("country", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("date", FieldType.DATE).index().facetAs(DateHandling.DATE_YYYY_MM_DD));

		lumongoWorkPool.createIndex(FACET_TEST_INDEX, 1, indexConfig);
	}

	@Test
	public void test03Facet() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		int id = 0;
		{
			for (String issn : issns) {
				for (int i = 0; i < COUNT_PER_ISSN; i++) {
					boolean half = (i % 2 == 0);
					boolean tenth = (i % 10 == 0);

					id++;

					String uniqueId = uniqueIdPrefix + id;

					DBObject object = new BasicDBObject();
					object.put("issn", issn);
					object.put("title", "Facet Userguide");

					if (half) { // 1/2 of input
						object.put("country", "US");
					}
					else { // 1/2 of input
						object.put("country", "France");
					}

					if (tenth) { // 1/10 of input

						Date d = Date.from(LocalDate.of(2014, Month.OCTOBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
						object.put("date", d);
					}
					else if (half) { // 2/5 of input
						Date d = Date.from(LocalDate.of(2013, Month.SEPTEMBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
						object.put("date", d);
					}
					else { // 1/2 of input
						Date d = Date.from(LocalDate.of(2013, 8, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
						object.put("date", d);
					}

					Store s = new Store(uniqueId, FACET_TEST_INDEX);
					s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(object));

					lumongoWorkPool.store(s);
				}
			}
		}

	}

	@Test
	public void test04Restart() throws Exception {
		stopClient();
		stopServer();
		MongoFile.clearCache();
		Thread.sleep(20000);
		startServer(1);
		startClient();
	}

	@Test
	public void test05ConfirmCounts() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest("issn", 30);
			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count not " + totalRecords, totalRecords, qr.getTotalHits());

			assertEquals("Total facets not " + issns.length, qr.getFacetCounts("issn").size(), issns.length);
			for (FacetCount fc : qr.getFacetCounts("issn")) {
				System.out.println(fc.getFacet() + ": " + fc.getCount());
				assertEquals("Count for facet <" + fc.getFacet() + "> not <" + COUNT_PER_ISSN + ">", COUNT_PER_ISSN, fc.getCount());
			}

		}

		{

			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest("date", 30);
			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count not " + totalRecords, totalRecords, qr.getTotalHits());

			assertEquals("Total facets not " + 3, 3, qr.getFacetCounts("date").size());
			for (@SuppressWarnings("unused") FacetCount fc : qr.getFacetCounts("date")) {
				//System.out.println(fc);
			}

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2014-10-04");

			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count after drill down not " + totalRecords / 10, totalRecords / 10, qr.getTotalHits());

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2013-09-04");

			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count after drill down not " + (totalRecords * 2) / 5, (totalRecords * 2) / 5, qr.getTotalHits());

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2013-08-04");

			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count after drill down not " + totalRecords / 2, totalRecords / 2, qr.getTotalHits());

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234");

			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count after drill down not " + COUNT_PER_ISSN, COUNT_PER_ISSN, qr.getTotalHits());

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("issn", "3333-1234");

			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN * 2), COUNT_PER_ISSN * 2, qr.getTotalHits());
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("country", "France");

			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN / 2), COUNT_PER_ISSN / 2, qr.getTotalHits());
			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN / 2), COUNT_PER_ISSN / 2, qr.getTotalHits());
		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10);
			q.addDrillDown("issn", "1234-1234").addDrillDown("country", "France");
			q.addCountRequest("issn");

			QueryResult qr = lumongoWorkPool.query(q);

			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN / 2), COUNT_PER_ISSN / 2, qr.getTotalHits());
			assertEquals("Number of issn facets not equal " + 1, 1, qr.getFacetCounts("issn").size());
		}
	}

	@AfterClass
	public void test04Start() throws Exception {
		stopSuite();
	}
}
