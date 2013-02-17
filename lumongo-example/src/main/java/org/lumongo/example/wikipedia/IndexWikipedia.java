package org.lumongo.example.wikipedia;

import java.io.File;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.StoreResult;
import org.lumongo.fields.Mapper;
import org.lumongo.util.LogUtil;
import org.lumongo.xml.StaxJAXBReader;
import org.wikipedia.schema.ContributorType;
import org.wikipedia.schema.PageType;
import org.wikipedia.schema.RedirectType;
import org.wikipedia.schema.RevisionType;
import org.wikipedia.schema.TextType;


public class IndexWikipedia {

    @SuppressWarnings("unused")
    private final static Logger log = Logger.getLogger(IndexWikipedia.class);

    private static Mapper<Article> mapper;

    private static LumongoWorkPool lumongoWorkPool;



    public static void main(String[] args) throws Exception {

    	if (args.length != 1) {
    		System.out.println("Usage: filename");
    		System.out.println(" ex. /tmp/enwiki-20120802-pages-articles-multistream.xml");
    		System.out.println(" download from http://dumps.wikimedia.org/enwiki/20120802/enwiki-20120802-pages-articles-multistream.xml.bz2");
    		System.exit(1);
    	}

    	String fileName = args[0];

    	if (!(new File(fileName)).exists()) {
    		System.out.println("File <" + fileName + "> does not exist");
    		System.exit(2);
    	}

    	LogUtil.loadLogConfig();


        lumongoWorkPool = new LumongoWorkPool(new LumongoPoolConfig().addMember("localhost"));
        mapper = new Mapper<Article>(Article.class);

        @SuppressWarnings("unused")
        CreateOrUpdateIndexResult createOrUpdateIndexResult = lumongoWorkPool.createOrUpdateIndex(mapper.createOrUpdateIndex());

        StaxJAXBReader<PageType> s = new StaxJAXBReader<PageType>(PageType.class, "page") {

            private int counter = 0;
            private long start = System.currentTimeMillis();
            private long last = System.currentTimeMillis();

            @Override
            public void handle(PageType item) throws Exception {
                final Article article = formArticle(item);
                if (article != null) {

                    Store store = mapper.createStore(article);

                    @SuppressWarnings("unused")
                    Future<StoreResult> sr = lumongoWorkPool.storeAsync(store);

                }
                if (++counter % 5000 == 0) {
                    long end = System.currentTimeMillis();
                    long timeForSet = end - last;
                    long timeSinceStart = end - start;
                    System.out.println(counter + "\t" + timeForSet + "\t" + timeSinceStart);
                    last = end;
                }
            }

        };

        s.handleFile(fileName);

        lumongoWorkPool.shutdown();
    }

    public static Article formArticle(final PageType page) {
        RedirectType redirectType = page.getRedirect();
        if (redirectType != null) {
            // skip redirects
            return null;
        }

        Article article = new Article();
        article.setTitle(page.getTitle());
        article.setNamespace(page.getNs().intValue());
        article.setId(page.getId().toString());

        List<Object> revUploadList = page.getRevisionOrUpload();

        for (Object o : revUploadList) {
            if (o instanceof RevisionType) {
                RevisionType revisionType = (RevisionType) o;

                TextType textType = revisionType.getText();
                article.setText(textType.getValue());
                article.setRevision(revisionType.getId().longValue());

                ContributorType contributorType = revisionType.getContributor();
                if (contributorType != null) {
                    if (contributorType.getId() != null) {
                        article.setUserId(contributorType.getId().intValue());
                    }
                    article.setUser(contributorType.getUsername());
                }
                article.setRevisionDate(revisionType.getTimestamp().toGregorianCalendar().getTime());

                break;
            }
        }
        return article;
    }




}
