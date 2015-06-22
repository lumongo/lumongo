package org.lumongo.test.cluster;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.joda.time.DateTime;
import org.lumongo.client.command.DeleteAllAssociated;
import org.lumongo.client.command.DeleteAssociated;
import org.lumongo.client.command.DeleteFull;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.FetchDocumentAndAssociated;
import org.lumongo.client.command.FetchLargeAssociated;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.doc.AssociatedBuilder;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.FieldConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class SingleNodeTest2 extends ServerTestBase {

	public static final String FACET_TEST_INDEX = "facetTestIndex";

	
	@BeforeClass
	public void test01Start() throws Exception {
		startSuite(1);
	}
	
	@Test
	public void test02Init() throws Exception {

		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();
		
		String defaultSearchField = "issn";
		IndexConfig indexConfig = new IndexConfig(defaultSearchField);

		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn").indexAs(LMAnalyzer.LC_KEYWORD).facetAs(LMFacetType.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("uid").indexAs(LMAnalyzer.LC_KEYWORD));


		lumongoWorkPool.createIndex(FACET_TEST_INDEX, 1, "uid", indexConfig);
	}
	
	@Test
	public void test03Facet() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		final int COUNT_PER_ISSN = 10;
		final String uniqueIdPrefix = "myId-";
		
		final String[] issns = new String[] { "1234-1234", "3333-1234", "1234-5555", "1234-4444", "2222-2222" };
		int totalRecords = COUNT_PER_ISSN * issns.length;
		
		int id = 0;
		{
			for (String issn : issns) {
				for (int i = 0; i < COUNT_PER_ISSN; i++) {

					id++;
					
					String uniqueId = uniqueIdPrefix + id;
					
					DBObject object = new BasicDBObject();
					object.put("issn", issn);

					

					Store s = new Store(uniqueId, FACET_TEST_INDEX);
					s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(object));
					
					lumongoWorkPool.store(s);
				}
			}
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "*:*", 10).addCountRequest(30, "issn");
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count not " + totalRecords, totalRecords, qr.getTotalHits());
			
			assertEquals("Total facets not " + issns.length, qr.getFacetCounts("issn").size(), issns.length);
			for (FacetCount fc : qr.getFacetCounts("issn")) {
				System.out.println(fc.getFacet() + ": " + fc.getCount());
				assertEquals("Count for facet <" + fc.getFacet() + "> not <" + COUNT_PER_ISSN + ">", COUNT_PER_ISSN, fc.getCount());
			}
			
		}
		



		
	}
	

	
	@AfterClass
	public void test10Shutdown() throws Exception {
		stopSuite();
	}
}
