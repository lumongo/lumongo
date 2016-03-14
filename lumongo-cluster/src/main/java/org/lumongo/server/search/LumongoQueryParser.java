package org.lumongo.server.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.lsh.LSH;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.Query;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.server.config.IndexConfig;

import java.io.IOException;
import java.io.StringReader;

public class LumongoQueryParser extends QueryParser {

	private static final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis();

	private static final DateTimeFormatter dateFormatter = ISODateTimeFormat.date();

	private IndexConfig indexConfig;

	private int minimumNumberShouldMatch;

	public LumongoQueryParser(Analyzer analyzer, IndexConfig indexConfig) {
		super(indexConfig.getDefaultSearchField(), analyzer);
		this.indexConfig = indexConfig;
		setAllowLeadingWildcard(true);
	}

	public void setField(String field) {
		if (field == null) {
			throw new IllegalArgumentException("Field can not be null");
		}
		this.field = field;
	}

	public void setMinimumNumberShouldMatch(int minimumNumberShouldMatch) {
		this.minimumNumberShouldMatch = minimumNumberShouldMatch;
	}

	@Override
	protected Query getRangeQuery(String field, String start, String end, boolean startInclusive, boolean endInclusive) throws ParseException {

		Lumongo.LMAnalyzer analyzer = indexConfig.getAnalyzer(field);
		if (IndexConfig.isNumericOrDateAnalyzer(analyzer)) {
			return getNumericOrDateRange(field, start, end, startInclusive, endInclusive);
		}

		return super.getRangeQuery(field, start, end, startInclusive, endInclusive);

	}

	private LegacyNumericRangeQuery<?> getNumericOrDateRange(final String fieldName, final String start, final String end, final boolean startInclusive,
			final boolean endInclusive) {
		Lumongo.LMAnalyzer analyzer = indexConfig.getAnalyzer(fieldName);
		if (IndexConfig.isNumericIntAnalyzer(analyzer)) {
			Integer min = start == null ? null : Integer.parseInt(start);
			Integer max = end == null ? null : Integer.parseInt(end);
			return LegacyNumericRangeQuery.newIntRange(fieldName, min, max, startInclusive, endInclusive);
		}
		else if (IndexConfig.isNumericLongAnalyzer(analyzer)) {
			Long min = start == null ? null : Long.parseLong(start);
			Long max = end == null ? null : Long.parseLong(end);
			return LegacyNumericRangeQuery.newLongRange(fieldName, min, max, startInclusive, endInclusive);
		}
		else if (IndexConfig.isNumericFloatAnalyzer(analyzer)) {
			Float min = start == null ? null : Float.parseFloat(start);
			Float max = end == null ? null : Float.parseFloat(end);
			return LegacyNumericRangeQuery.newFloatRange(fieldName, min, max, startInclusive, endInclusive);
		}
		else if (IndexConfig.isNumericDoubleAnalyzer(analyzer)) {
			Double min = start == null ? null : Double.parseDouble(start);
			Double max = end == null ? null : Double.parseDouble(end);
			return LegacyNumericRangeQuery.newDoubleRange(fieldName, min, max, startInclusive, endInclusive);
		}
		else if (IndexConfig.isDateAnalyzer(analyzer)) {
			Long startTime = null;
			Long endTime = null;
			if (start != null) {
				startTime = getDateAsLong(start);
			}
			if (end != null) {
				endTime = getDateAsLong(end);
			}
			return LegacyNumericRangeQuery.newLongRange(fieldName, startTime, endTime, startInclusive, endInclusive);
		}
		throw new RuntimeException("Not a valid numeric field <" + fieldName + ">");
	}

	private Long getDateAsLong(String dateString) {
		DateTime dateTime;
		if (dateString.contains(":")) {
			dateTime = dateTimeFormatter.parseDateTime(dateString);
		}
		else {
			dateTime = dateFormatter.parseDateTime(dateString);
		}
		return dateTime.toDate().getTime();
	}

	@Override
	protected Query newTermQuery(org.apache.lucene.index.Term term) {
		String field = term.field();
		String text = term.text();

		Lumongo.LMAnalyzer analyzer = indexConfig.getAnalyzer(field);
		if (IndexConfig.isNumericOrDateAnalyzer(analyzer)) {
			return getNumericOrDateRange(field, text, text, true, true);
		}

		return super.newTermQuery(term);
	}

	@Override
	protected BooleanQuery.Builder newBooleanQuery() {
		BooleanQuery.Builder builder = new BooleanQuery.Builder();
		builder.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
		return builder;
	}

	@Override
	protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
		Lumongo.LMAnalyzer lmAnalyzer = indexConfig.getAnalyzer(field);
		if (Lumongo.LMAnalyzer.LSH.equals(lmAnalyzer)) {
			try {
				float sim = slop / 100.0f;
				return LSH.createSlowQuery(getAnalyzer(), field, new StringReader(queryText), 100, sim);
			}
			catch (IOException e) {
				throw new ParseException(e.getMessage());
			}
		}
		return super.getFieldQuery(field, queryText, slop);
	}
}
