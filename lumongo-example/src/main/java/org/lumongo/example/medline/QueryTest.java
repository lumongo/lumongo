package org.lumongo.example.medline;


import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.fields.Mapper;
import org.lumongo.util.LogUtil;


public class QueryTest {
    public static void main(String[] args) throws Exception {
        LogUtil.loadLogConfig();

        LumongoWorkPool lumongoWorkPool = new LumongoWorkPool(new LumongoPoolConfig().addMember("localhost"));

        try {
            Mapper<Document> mapper = new Mapper<Document>(Document.class);

            Query query = new Query("medline", "title:water", 10);

            QueryResult queryResult = lumongoWorkPool.query(query);

            long totalHits = queryResult.getTotalHits();

            System.out.println("Found <" + totalHits + "> hits");
            for (ScoredResult sr : queryResult.getResults()) {

            	FetchResult fr = lumongoWorkPool.fetch(new FetchDocument(sr));

            	Document d = fr.getDocument(mapper);

                System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + "> <" + d.getIssn() + ">");
            }
        }
        finally {
            lumongoWorkPool.shutdown();
        }
    }
}
