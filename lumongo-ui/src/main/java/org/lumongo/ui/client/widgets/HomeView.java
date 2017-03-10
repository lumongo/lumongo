package org.lumongo.ui.client.widgets;

import com.google.gwt.core.client.Scheduler;
import gwt.material.design.client.ui.html.Div;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.charts.PieChart;
import org.lumongo.ui.shared.InstanceInfo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Payam Meyer on 3/9/17.
 * @author pmeyer
 */
public class HomeView extends Div {

	public void drawSplashPage(InstanceInfo instanceInfo) {
		clear();

		// show some info:
		// available memory
		// lumongo version
		// lucene version

		Map<String, Serializable> data = new HashMap<>();
		data.put("publications", 35);
		data.put("grants", 35);
		data.put("rfi", 35);
		data.put("uspto", 35);
		Scheduler.get().scheduleDeferred(() -> {
			Highcharts chart = PieChart.getBuilder().setChartTitle("Some Title").setHeight(320).setData(data).setYAxisAllowDecimals(false).build();
			add(chart);
		});

	}
}
