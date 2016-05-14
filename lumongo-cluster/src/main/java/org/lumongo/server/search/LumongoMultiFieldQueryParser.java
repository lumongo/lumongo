package org.lumongo.server.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.lumongo.server.config.IndexConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by Matt Davis on 5/14/16.
 * @author mdavis
 * Copied mostly from org.apache.lucene.queryparser.classic.MultiFieldQueryParser
 */
public class LumongoMultiFieldQueryParser extends LumongoQueryParser {

	protected Collection<String> fields;
	protected Map<String,Float> boosts;

	public LumongoMultiFieldQueryParser(Analyzer analyzer, IndexConfig indexConfig) {
		super(analyzer, indexConfig);
	}

	public void setDefaultFields(Collection<String> fields) {
		setDefaultFields(fields, null);
	}

	public void setDefaultFields(Collection<String> fields, Map<String, Float> boosts) {
		this.field = null;
		this.fields = fields;
		this.boosts = boosts;
	}

	@Override
	public void setDefaultField(String field) {
		super.setDefaultField(field);
		this.fields = null;
	}

	@Override
	protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f: fields) {
				Query q = super.getFieldQuery(f, queryText, true);
				if (q != null) {
					//If the user passes a map of boosts
					if (boosts != null) {
						//Get the boost from the map and apply them
						Float boost = boosts.get(f);
						if (boost != null) {
							q = new BoostQuery(q, boost);
						}
					}
					q = applySlop(q,slop);
					clauses.add(q);
				}
			}
			if (clauses.size() == 0)  // happens for stopwords
				return null;
			return getMultiFieldQuery(clauses);
		}
		Query q = super.getFieldQuery(field, queryText, true);
		q = applySlop(q,slop);
		return q;
	}

	private Query applySlop(Query q, int slop) {
		if (q instanceof PhraseQuery) {
			PhraseQuery.Builder builder = new PhraseQuery.Builder();
			builder.setSlop(slop);
			PhraseQuery pq = (PhraseQuery) q;
			org.apache.lucene.index.Term[] terms = pq.getTerms();
			int[] positions = pq.getPositions();
			for (int i = 0; i < terms.length; ++i) {
				builder.add(terms[i], positions[i]);
			}
			q = builder.build();
		} else if (q instanceof MultiPhraseQuery) {
			MultiPhraseQuery mpq = (MultiPhraseQuery)q;

			if (slop != mpq.getSlop()) {
				q = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
			}
		}
		return q;
	}


	@Override
	protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				Query q = super.getFieldQuery(f, queryText, quoted);
				if (q != null) {
					//If the user passes a map of boosts
					if (boosts != null) {
						//Get the boost from the map and apply them
						Float boost = boosts.get(f);
						if (boost != null) {
							q = new BoostQuery(q, boost);
						}
					}
					clauses.add(q);
				}
			}
			if (clauses.size() == 0)  // happens for stopwords
				return null;
			return getMultiFieldQuery(clauses);
		}
		Query q = super.getFieldQuery(field, queryText, quoted);
		return q;
	}


	@Override
	protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException
	{
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f: fields) {
				clauses.add(getFuzzyQuery(f, termStr, minSimilarity));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getFuzzyQuery(field, termStr, minSimilarity);
	}

	@Override
	protected Query getPrefixQuery(String field, String termStr) throws ParseException
	{
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f: fields) {
				clauses.add(getPrefixQuery(f, termStr));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getPrefixQuery(field, termStr);
	}

	@Override
	protected Query getWildcardQuery(String field, String termStr) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f: fields) {
				clauses.add(getWildcardQuery(f, termStr));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getWildcardQuery(field, termStr);
	}


	@Override
	protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				clauses.add(getRangeQuery(f, part1, part2, startInclusive, endInclusive));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
	}



	@Override
	protected Query getRegexpQuery(String field, String termStr)
			throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				clauses.add(getRegexpQuery(f, termStr));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getRegexpQuery(field, termStr);
	}

	/** Creates a multifield query */
	// TODO: investigate more general approach by default, e.g. DisjunctionMaxQuery?
	protected Query getMultiFieldQuery(List<Query> queries) throws ParseException {
		if (queries.isEmpty()) {
			return null; // all clause words were filtered away by the analyzer.
		}
		//mdavis - don't use super method because of min match
		BooleanQuery.Builder query = new BooleanQuery.Builder();
		query.setDisableCoord(true);
		for (Query sub : queries) {
			query.add(sub, BooleanClause.Occur.SHOULD);
		}
		return query.build();
	}

}
