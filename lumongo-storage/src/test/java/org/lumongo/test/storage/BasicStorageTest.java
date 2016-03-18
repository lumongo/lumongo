package org.lumongo.test.storage;

import com.mongodb.MongoClient;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.TestHelper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class BasicStorageTest {
	private static final String STORAGE_TEST_INDEX = "storageTest";
	private static Directory directory;

	@BeforeClass
	public static void cleanDatabaseAndInit() throws Exception {

		MongoClient mongo = TestHelper.getMongo();
		mongo.dropDatabase(TestHelper.TEST_DATABASE_NAME);
		directory = new DistributedDirectory(new MongoDirectory(mongo, TestHelper.TEST_DATABASE_NAME, STORAGE_TEST_INDEX, false));

		StandardAnalyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);

		IndexWriter w = new IndexWriter(directory, config);

		addDoc(w, "Random perl Title that is long", "id-1");
		addDoc(w, "Random java Title that is long", "id-1");
		addDoc(w, "MongoDB is awesome", "id-2");
		addDoc(w, "This is a long title with nothing interesting", "id-3");
		addDoc(w, "Java is awesome", "id-4");
		addDoc(w, "Really big fish", "id-5");

		w.commit();
		w.close();
	}

	@AfterClass
	public static void closeDirectory() throws Exception {
		directory.close();
	}

	private static void addDoc(IndexWriter w, String title, String uid) throws IOException {
		Document doc = new Document();
		doc.add(new TextField("title", title, Field.Store.YES));
		doc.add(new TextField("uid", uid, Field.Store.YES));
		doc.add(new StringField("uid", uid, Field.Store.YES));
		doc.add(new IntPoint("testIntField", 3));
		long date = System.currentTimeMillis();
		doc.add(new LongPoint("date", date));
		doc.add(new NumericDocValuesField("date", date));
		doc.add(new SortedSetDocValuesField("category", new BytesRef("Anything")));
		Term uidTerm = new Term("uid", uid);

		w.updateDocument(uidTerm, doc);
	}

	private static int runQuery(IndexReader indexReader, QueryParser qp, String queryStr, int count) throws IOException, ParseException {

		Query q = qp.parse(queryStr);

		return runQuery(indexReader, count, q);

	}

	private static int runQuery(IndexReader indexReader, int count, Query q) throws IOException {
		long start = System.currentTimeMillis();
		IndexSearcher searcher = new IndexSearcher(indexReader);

		Sort sort = new Sort();

		sort.setSort(new SortedSetSortField("category", false));

		TopFieldCollector collector = TopFieldCollector.create(sort, count, null, true, true, true);

		searcher.search(q, collector);

		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		int totalHits = collector.getTotalHits();
		@SuppressWarnings("unused") long searchTime = System.currentTimeMillis() - start;

		start = System.currentTimeMillis();

		List<String> ids = new ArrayList<>();
		for (ScoreDoc hit : hits) {
			int docId = hit.doc;
			Document d = searcher.doc(docId);
			ids.add(d.get("uid"));

		}
		@SuppressWarnings("unused") long fetchTime = System.currentTimeMillis() - start;

		return totalHits;
	}

	@Test
	public void test2Query() throws IOException, ParseException {
		IndexReader indexReader = DirectoryReader.open(directory);

		StandardAnalyzer analyzer = new StandardAnalyzer();
		QueryParser qp = new QueryParser("title", analyzer) {

			@Override
			protected Query getRangeQuery(final String fieldName, final String start, final String end, final boolean startInclusive,
					final boolean endInclusive) throws ParseException {

				if ("testIntField".equals(fieldName)) {
					int startInt = Integer.parseInt(start);
					int endInt = Integer.parseInt(end);
					if (!startInclusive) {
						startInt += 1;
					}
					if (!endInclusive) {
						endInt -= 1;
					}
					return IntPoint.newRangeQuery(fieldName, startInt, endInt);

				}

				// return default
				return super.getRangeQuery(fieldName, start, end, startInclusive, endInclusive);

			}

			@Override
			protected Query newTermQuery(org.apache.lucene.index.Term term) {
				String field = term.field();
				String text = term.text();
				if ("testIntField".equals(field)) {
					int value = Integer.parseInt(text);
					return IntPoint.newExactQuery(field, value);
				}
				return super.newTermQuery(term);
			}
		};
		qp.setAllowLeadingWildcard(true);

		int hits;

		hits = runQuery(indexReader, qp, "java", 10);
		assertEquals("Expected 2 hits", 2, hits);
		hits = runQuery(indexReader, qp, "perl", 10);
		assertEquals("Expected 0 hits", 0, hits);
		hits = runQuery(indexReader, qp, "treatment", 10);
		assertEquals("Expected 0 hits", 0, hits);
		hits = runQuery(indexReader, qp, "long", 10);
		assertEquals("Expected 2 hits", 2, hits);
		hits = runQuery(indexReader, qp, "MongoDB", 10);
		assertEquals("Expected 1 hit", 1, hits);
		hits = runQuery(indexReader, qp, "java AND awesome", 10);
		assertEquals("Expected 1 hit", 1, hits);
		hits = runQuery(indexReader, qp, "testIntField:1", 10);
		assertEquals("Expected 0 hits", 0, hits);
		hits = runQuery(indexReader, qp, "testIntField:3", 10);
		assertEquals("Expected 5 hits", 5, hits);
		hits = runQuery(indexReader, qp, "testIntField:[1 TO 10]", 10);
		assertEquals("Expected 5 hits", 5, hits);


		indexReader.close();
	}

	@Test
	public void test3Api() throws Exception {
		String hostName = TestHelper.getMongoServer();
		String databaseName = TestHelper.TEST_DATABASE_NAME;

		{

			MongoClient mongo = new MongoClient(hostName);
			Directory directory = new DistributedDirectory(new MongoDirectory(mongo, databaseName, STORAGE_TEST_INDEX));

			StandardAnalyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			IndexWriter w = new IndexWriter(directory, config);

			boolean applyDeletes = true;

			IndexReader ir = DirectoryReader.open(w, applyDeletes, false);

			ir.close();

			w.commit();
			w.close();

			directory.close();

		}

		{

			MongoClient mongo = new MongoClient(hostName);
			Directory d = new DistributedDirectory(new MongoDirectory(mongo, databaseName, STORAGE_TEST_INDEX));
			IndexReader indexReader = DirectoryReader.open(d);
			indexReader.close();
			d.close();

		}

		{
			MongoClient mongo = new MongoClient(hostName);
			DistributedDirectory d = new DistributedDirectory(new MongoDirectory(mongo, databaseName, STORAGE_TEST_INDEX));

			File f = new File("/tmp/fsdirectory");
			f.mkdirs();

			d.copyToFSDirectory(f.toPath());

			d.close();
		}
	}
}
