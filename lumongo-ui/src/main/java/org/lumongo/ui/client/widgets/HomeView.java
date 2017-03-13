package org.lumongo.ui.client.widgets;

import gwt.material.design.client.constants.Color;
import gwt.material.design.client.ui.MaterialCard;
import gwt.material.design.client.ui.MaterialCardContent;
import gwt.material.design.client.ui.MaterialCardTitle;
import gwt.material.design.client.ui.MaterialColumn;
import gwt.material.design.client.ui.MaterialLabel;
import gwt.material.design.client.ui.MaterialRow;
import gwt.material.design.client.ui.html.Div;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.charts.BarChart;
import org.lumongo.ui.shared.IndexInfo;
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

		MaterialRow row = new MaterialRow();
		add(row);

		MaterialColumn column = new MaterialColumn();
		column.setGrid("l4");
		row.add(column);

		MaterialCard infoCard = new MaterialCard();
		infoCard.setBackgroundColor(Color.GREY_DARKEN_1);
		infoCard.setTextColor(Color.WHITE);
		MaterialCardContent infoContent = new MaterialCardContent();
		infoCard.add(infoContent);
		column.add(infoCard);

		MaterialCardTitle infoCardTitle = new MaterialCardTitle();
		infoCardTitle.setText("Basic Info");
		infoContent.add(infoCardTitle);

		MaterialLabel lumongoVersionLabel = new MaterialLabel("LuMongo Version: " + instanceInfo.getLumongoVersion());
		MaterialLabel luceneVersionLabel = new MaterialLabel("Lucene Version: " + instanceInfo.getLuceneVersion());
		MaterialLabel lumongoMemoryLabel = new MaterialLabel("LuMongo Memory: " + instanceInfo.getLumongoMemory());
		MaterialLabel serverMemoryLabel = new MaterialLabel("Server Memory: " + instanceInfo.getServerMemory());
		MaterialLabel diskSpaceLabel = new MaterialLabel("Disk Space: " + instanceInfo.getDiskSize());

		infoContent.add(lumongoVersionLabel);
		infoContent.add(luceneVersionLabel);
		infoContent.add(lumongoMemoryLabel);
		infoContent.add(serverMemoryLabel);
		infoContent.add(diskSpaceLabel);

		Map<String, Serializable> data = new HashMap<>();
		for (IndexInfo indexInfo : instanceInfo.getIndexes()) {
			data.put(indexInfo.getName(), indexInfo.getTotalDocs());
		}
		Highcharts chart = BarChart.getBuilder().setChartTitle("Index Info").setHeight(400).setData(data).setYAxisAllowDecimals(false).build();
		column.add(chart);

	}
}
