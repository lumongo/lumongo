package org.lumongo.ui.client.charting.handlers;

import org.lumongo.ui.client.charting.options.Series;

public interface TooltipFormatter {

	String format(String x, String y, Series series);
}
