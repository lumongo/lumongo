package org.lumongo.ui.client.charting;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.user.client.ui.FlowPanel;
import org.lumongo.ui.client.charting.options.ChartOptions;
import org.lumongo.ui.client.charting.options.CreditOptions;
import org.lumongo.ui.client.charting.options.ExportingOptions;
import org.lumongo.ui.client.charting.options.HighchartsOptions;

public class Highcharts extends FlowPanel {

	private static String exportUrl;

	public static void setExportUrl(String exportUrl) {
		Highcharts.exportUrl = exportUrl;
	}

	private String id;

	private HighchartsOptions highchartsOptions;

	private final Integer height;

	public Highcharts() {
		this(null);
	}

	public Highcharts(Integer height) {
		this.id = generateId();
		getElement().setId(id);
		this.height = height;
		this.addAttachHandler(event -> {
			if (event.isAttached()) {
				//Window.alert("attached");
				draw();
				//FIXME: Need to handle the size of the chart so multiple charts don't overwrite each other.
			}
		});
	}

	protected void setHighchartOptions(HighchartsOptions highchartsOptions) {
		this.highchartsOptions = highchartsOptions;
	}

	public HighchartsOptions getHighchartsOptions() {
		return highchartsOptions;
	}

	protected static final native String generateId() /*-{
        var CHARS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'.split('');
        //http://www.broofa.com/Tools/Math.uuid.js
        var chars = CHARS, uuid = new Array(36), rnd = 0, r;
        for (var i = 0; i < 36; i++) {
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                uuid[i] = '-';
            } else if (i == 14) {
                uuid[i] = '4';
            } else {
                if (rnd <= 0x02) rnd = 0x2000000 + (Math.random() * 0x1000000) | 0;
                r = rnd & 0xf;
                rnd = rnd >> 4;
                uuid[i] = chars[(i == 19) ? (r & 0x3) | 0x8 : r];
            }
        }
        return uuid.join('');
    }-*/;

	public void draw() {
		if (highchartsOptions.getCredits() == null) {
			highchartsOptions.setCredits(CreditOptions.create());
			highchartsOptions.getCredits().setEnabled(false);
		}

		if (highchartsOptions.getChart() == null) {
			highchartsOptions.setChart(ChartOptions.create());
		}
		highchartsOptions.getChart().setRenderTo(id);

		if (highchartsOptions.getExporting() == null) {
			highchartsOptions.setExporting(ExportingOptions.create());
		}
		highchartsOptions.getExporting().setURL(exportUrl);

		if (height != null) {
			highchartsOptions.getChart().setHeight(height);
		}

		try {
			executeDraw();
		}
		catch (Exception e) {
			GWT.log("Chart Failed to Build:: " + e.getMessage());
		}
	}

	protected void executeDraw() {
		draw(highchartsOptions);
	}

	private JavaScriptObject chart;

	protected final native void draw(HighchartsOptions highchartsOptions) /*-{
        this.@org.lumongo.ui.client.charting.Highcharts::chart = new $wnd.Highcharts.Chart(highchartsOptions);

    }-*/;

	public final native void reflow()/*-{
        this.@org.lumongo.ui.client.charting.Highcharts::chart.reflow();
    }-*/;

}
