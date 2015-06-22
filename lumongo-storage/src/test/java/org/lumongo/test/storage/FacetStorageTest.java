package org.lumongo.test.storage;

import com.mongodb.MongoClient;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.TestHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mdavis on 6/21/15.
 */
public class FacetStorageTest {
	private static final String STORAGE_TEST_INDEX = "storageTest";
	private static Directory directory;
	
	private final FacetsConfig config = new FacetsConfig();


	public static void init() throws IOException {
		MongoClient mongo = TestHelper.getMongo();
		mongo.dropDatabase(TestHelper.TEST_DATABASE_NAME);
		directory = new DistributedDirectory(new MongoDirectory(mongo, TestHelper.TEST_DATABASE_NAME, STORAGE_TEST_INDEX, false));
	}

	/** Build the example index. */
	private void index() throws IOException {
		config.setMultiValued("Author", true);
		IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(
						new WhitespaceAnalyzer()));
		Document doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Bob"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2010"));
		indexWriter.addDocument(config.build(doc));
		
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Lisa"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2010"));
		indexWriter.addDocument(config.build(doc));
		
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Lisa"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2012"));
		indexWriter.addDocument(config.build(doc));
		
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Susan"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2012"));
		indexWriter.addDocument(config.build(doc));
		
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Frank"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "1999"));
		indexWriter.addDocument(config.build(doc));
		
		indexWriter.close();
	}
	
	/** User runs a query and counts facets. */
	private List<FacetResult> search() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(directory);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(indexReader);
		
		// Aggregates the facet counts
		FacetsCollector fc = new FacetsCollector();
		
		// MatchAllDocsQuery is for "browsing" (counts facets
		// for all non-deleted docs in the index); normally
		// you'd use a "normal" query:
		//FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

		TotalHitCountCollector collector = new TotalHitCountCollector();
		searcher.search(new MatchAllDocsQuery(), MultiCollector.wrap(collector, fc));
		
		// Retrieve results
		Facets facets = new SortedSetDocValuesFacetCounts(state, fc);
		
		List<FacetResult> results = new ArrayList<>();
		results.add(facets.getTopChildren(10, "Author"));
		results.add(facets.getTopChildren(10, "Publish Year"));
		indexReader.close();
		
		return results;
	}
	
	/** User drills down on 'Publish Year/2010'. */
	private FacetResult drillDown() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(directory);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(indexReader);
		
		// Now user drills down on Publish Year/2010:
		DrillDownQuery q = new DrillDownQuery(config);
		q.add("Publish Year", "2010");
		FacetsCollector fc = new FacetsCollector();
		FacetsCollector.search(searcher, q, 10, fc);
		
		// Retrieve results
		Facets facets = new SortedSetDocValuesFacetCounts(state, fc);
		FacetResult result = facets.getTopChildren(10, "Author");
		indexReader.close();
		
		return result;
	}
	
	/** Runs the search example. */
	public List<FacetResult> runSearch() throws IOException {
		index();
		return search();
	}
	
	/** Runs the drill-down example. */
	public FacetResult runDrillDown() throws IOException {
		index();
		return drillDown();
	}
	
	/** Runs the search and drill-down examples and prints the results. */
	public static void main(String[] args) throws Exception {
		init();

		System.out.println("Facet counting example:");
		System.out.println("-----------------------");
		FacetStorageTest example = new FacetStorageTest();
		List<FacetResult> results = example.runSearch();
		System.out.println("Author: " + results.get(0));
		System.out.println("Publish Year: " + results.get(1));
		
		System.out.println("\n");
		System.out.println("Facet drill-down example (Publish Year/2010):");
		System.out.println("---------------------------------------------");
		System.out.println("Author: " + example.runDrillDown());
	}
}
