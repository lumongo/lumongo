package org.lumongo.server.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.lumongo.server.config.IndexConfig;

public class LumongoQueryParser extends QueryParser {
	
	private static final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTimeNoMillis();
	
	private IndexConfig indexConfig;
	
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
	
	@Override
	protected Query getRangeQuery(String field, String start, String end, boolean startInclusive, boolean endInclusive) throws ParseException {
		
		if (indexConfig.isNumericOrDateField(field)) {
			return getNumericOrDateRange(field, start, end, startInclusive, endInclusive);
		}
		
		return super.getRangeQuery(field, start, end, startInclusive, endInclusive);
		
	}
	
	private NumericRangeQuery<?> getNumericOrDateRange(final String fieldName, final String start, final String end, final boolean startInclusive,
					final boolean endInclusive) {
		if (indexConfig.isNumericIntField(fieldName)) {
			return NumericRangeQuery.newIntRange(fieldName, start == null ? null : Integer.parseInt(start), end == null ? null : Integer.parseInt(end),
							startInclusive, endInclusive);
		}
		else if (indexConfig.isNumericLongField(fieldName)) {
			return NumericRangeQuery.newLongRange(fieldName, start == null ? null : Long.parseLong(start), end == null ? null : Long.parseLong(end),
							startInclusive, endInclusive);
		}
		else if (indexConfig.isNumericFloatField(fieldName)) {
			return NumericRangeQuery.newFloatRange(fieldName, start == null ? null : Float.parseFloat(start), end == null ? null : Float.parseFloat(end),
							startInclusive, endInclusive);
		}
		else if (indexConfig.isNumericDoubleField(fieldName)) {
			return NumericRangeQuery.newDoubleRange(fieldName, start == null ? null : Double.parseDouble(start), end == null ? null : Double.parseDouble(end),
							startInclusive, endInclusive);
		}
		else if (indexConfig.isDateField(fieldName)) {
			Long startTime = null;
			Long endTime = null;
			if (start != null) {
				DateTime startDate = dateFormatter.parseDateTime(start);
				startTime = startDate.toDate().getTime();
			}
			if (end != null) {
				DateTime endDate = dateFormatter.parseDateTime(end);
				endTime = endDate.toDate().getTime();
			}
			return NumericRangeQuery.newLongRange(fieldName, startTime, endTime, startInclusive, endInclusive);
		}
		throw new RuntimeException("Not a valid numeric field <" + fieldName + ">");
	}
	
	@Override
	protected Query newTermQuery(org.apache.lucene.index.Term term) {
		String field = term.field();
		String text = term.text();
		
		if (indexConfig.isNumericOrDateField(field)) {
			return getNumericOrDateRange(field, text, text, true, true);
		}
		
		return super.newTermQuery(term);
	}
}
