package org.lumongo.server.search;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.lsh.LSH;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
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

	private Query getNumericOrDateRange(final String fieldName, final String start, final String end, final boolean startInclusive,
			final boolean endInclusive) {
		Lumongo.LMAnalyzer analyzer = indexConfig.getAnalyzer(fieldName);
		if (IndexConfig.isNumericIntAnalyzer(analyzer)) {
			int min = start == null ? Integer.MIN_VALUE : Integer.parseInt(start);
			int max = end == null ? Integer.MAX_VALUE : Integer.parseInt(end);
			if (!startInclusive) {
				min += 1;
			}
			if (!endInclusive) {
				max -= 1;
			}
			return IntPoint.newRangeQuery(fieldName, min, max);
		}
		else if (IndexConfig.isNumericLongAnalyzer(analyzer)) {
			long min = start == null ? Long.MIN_VALUE : Long.parseLong(start);
			long max = end == null ? Long.MAX_VALUE : Long.parseLong(end);
			if (!startInclusive) {
				min += 1;
			}
			if (!endInclusive) {
				max -= 1;
			}
			return LongPoint.newRangeQuery(fieldName, min, max);
		}
		else if (IndexConfig.isNumericFloatAnalyzer(analyzer)) {
			float min = start == null ? Float.NEGATIVE_INFINITY : Float.parseFloat(start);
			float max = end == null ? Float.POSITIVE_INFINITY : Float.parseFloat(end);
			if (!startInclusive) {
				min += Math.nextUp(min);
			}
			if (!endInclusive) {
				max -= Math.nextDown(max);
			}
			return FloatPoint.newRangeQuery(fieldName, min, max);
		}
		else if (IndexConfig.isNumericDoubleAnalyzer(analyzer)) {
			double min = start == null ? Double.NEGATIVE_INFINITY : Double.parseDouble(start);
			double max = end == null ? Double.POSITIVE_INFINITY : Double.parseDouble(end);
			if (!startInclusive) {
				min += Math.nextUp(min);
			}
			if (!endInclusive) {
				max -= Math.nextDown(max);
			}
			return DoublePoint.newRangeQuery(fieldName, min, max);
		}
		else if (IndexConfig.isDateAnalyzer(analyzer)) {
			long min = Long.MIN_VALUE;
			long max = Long.MAX_VALUE;
			if (start != null) {
				min = getDateAsLong(start);
			}
			if (end != null) {
				max = getDateAsLong(end);
			}
			if (!startInclusive) {
				min += 1;
			}
			if (!endInclusive) {
				max -= 1;
			}
			return LongPoint.newRangeQuery(fieldName, min, max);
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
			if (Doubles.tryParse(text) != null) {
				return getNumericOrDateRange(field, text, text, true, true);
			}
			return null;
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
