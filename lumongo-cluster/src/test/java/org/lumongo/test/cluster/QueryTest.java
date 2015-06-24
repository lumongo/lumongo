package org.lumongo.test.cluster;

import com.google.common.util.concurrent.ListenableFuture;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoBaseWorkPool;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.result.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class QueryTest {
	public static void main(String[] args) throws Exception {
		
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.addMember("localhost", 32191);
		LumongoBaseWorkPool lumongoWorkPool = new LumongoBaseWorkPool(new LumongoPool(lumongoPoolConfig));

		String[] queries = new String[] { "human", "male", "female", "animal", "age", "protein", "cell", "factor", "disease", "agent", "acid", "dna", "child",
						"receptor", "drug", "liver", "mutation", "rabbits", "neurons", "brain", "variance", "apoptosis", "mucosal", "infection", "death",
						"cancer", "response", "heart", "treatment", "outcome", "middle", "survival", "analysis", "valine", "circulation", "complex", "angina",
						"unstable", "myocardial", "controlled", "trials", "platelet", "translational", "psoriasis", "adolescent", "health", "dermatology",
						"blood", "plasma", "acne", "adherence", "stroke", "aspirin", "migraine", "hyperphagia", "hiv", "melanoma", "prostate", "aggression",
						"arthritis", "abatacept", "enteritis", "histone", "lenalidomide", "potassium", "bortezomib", "lung", "enteritis", "pregnant",
						"myeloma", "paclitaxel", "cisplatin", "erlotinib", "antigens", "carbon", "sorafenib", "paclitaxel", "genetics", "embryo", "stem",
						"gemcitabine", "tumor", "alpha", "histone", "cetuximab", "steroids", "lead", "melphalan", "glucose", "radical", "taxane", "folic",
						"somatic", "perifosine", "carcinogens", "irinotecan", "communicable", "vomiting", "volatile", "water", "vaccination", "urology",
						"ultrasonic", "tyrosine", "tuberculosis", "triphosphate", "transplantable", "toxoplasmosis" };
		
		Query q = new Query("medline", "cancer", 25);
		
		long start = System.currentTimeMillis();
		List<ListenableFuture<QueryResult>> results = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			int index = ThreadLocalRandom.current().nextInt(queries.length);
			String query = queries[index];
			q.setQuery(query);
			
			ListenableFuture<QueryResult> qr = lumongoWorkPool.executeAsync(q);
			results.add(qr);

		}
		
		for (ListenableFuture<QueryResult> sr : results) {
			sr.get();
		}
		
		long end = System.currentTimeMillis();
		System.out.println(end - start);
		
		lumongoWorkPool.shutdown();
	}
}
