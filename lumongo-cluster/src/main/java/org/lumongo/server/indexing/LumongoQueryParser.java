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
			return NumericRangeQuery.newIntRange(fieldName, Integer.parseInt(start), Integer.parseInt(end), startInclusive, endInclusive);
		}
		else if (indexConfig.isNumericLongField(fieldName)) {
			return NumericRangeQuery.newLongRange(fieldName, Long.parseLong(start), Long.parseLong(end), startInclusive, endInclusive);
		}
		else if (indexConfig.isNumericFloatField(fieldName)) {
			return NumericRangeQuery.newFloatRange(fieldName, Float.parseFloat(start), Float.parseFloat(end), startInclusive, endInclusive);
		}
		else if (indexConfig.isNumericDoubleField(fieldName)) {
			return NumericRangeQuery.newDoubleRange(fieldName, Double.parseDouble(start), Double.parseDouble(end), startInclusive, endInclusive);
		}
		else if (indexConfig.isDateField(fieldName)) {
			DateTime startDate = dateFormatter.parseDateTime(start);
			DateTime endDate = dateFormatter.parseDateTime(end);
			
			return NumericRangeQuery.newLongRange(fieldName, startDate.toDate().getTime(), endDate.toDate().getTime(), startInclusive, endInclusive);
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
