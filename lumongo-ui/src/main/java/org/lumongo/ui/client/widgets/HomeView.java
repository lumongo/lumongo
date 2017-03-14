package org.lumongo.ui.client.widgets;

import gwt.material.design.client.constants.Color;
import gwt.material.design.client.ui.MaterialCard;
import gwt.material.design.client.ui.MaterialCardContent;
import gwt.material.design.client.ui.MaterialCardTitle;
import gwt.material.design.client.ui.MaterialColumn;
import gwt.material.design.client.ui.MaterialLabel;
import gwt.material.design.client.ui.MaterialPanel;
import gwt.material.design.client.ui.MaterialRow;
import gwt.material.design.client.ui.html.Div;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.charts.PieChart;
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

		MaterialPanel panel = new MaterialPanel();
		MaterialRow row = new MaterialRow();
		panel.add(row);
		add(panel);

		MaterialColumn infoColumn = new MaterialColumn();
		infoColumn.setGrid("s12 m6");
		row.add(infoColumn);

		MaterialCard infoCard = new MaterialCard();
		infoCard.setBackgroundColor(Color.GREY_DARKEN_1);
		infoCard.setTextColor(Color.WHITE);
		MaterialCardContent infoContent = new MaterialCardContent();
		infoCard.add(infoContent);
		infoColumn.add(infoCard);

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

		MaterialColumn chartColumn = new MaterialColumn();
		chartColumn.setGrid("s12 m6");
		Map<String, Serializable> data = new HashMap<>();
		for (IndexInfo indexInfo : instanceInfo.getIndexes()) {
			data.put(indexInfo.getName(), indexInfo.getTotalDocs());
		}
		Highcharts chart = PieChart.getBuilder().setChartTitle("Index Info").setHeight(400).setData(data).setYAxisAllowDecimals(false).build();
		chartColumn.add(chart);
		row.add(chartColumn);

		Div div = new Div();
		div.add(new MaterialLabel("{ \n" + "    domain: \"127.0.0.1\",\n" + "    https: false,\n" + "    localUsers: true,\n" + "    disableSAML: true,\n"
				+ "    userCollection: \"local\",\n" + "    exportUrl: \"http://localhost:9999/dataprocessing/export\",\n"
				+ "    portfolioUrl: \"http://localhost:9999/dataprocessing/portfolio\",\n"
				+ "    downloadUrl: \"http://localhost:9999/dataprocessing/download\",\n"
				+ "    downloadChartsUrl: \"http://localhost:9999/dataprocessing/charts\",\n" + "    lingoBaseUrl: \"https://admin.lexicalintelligence.com/\"\n"
				+ "}\n"));

		row.add(div);

		//Highlight.e(div.getElement());

		//HighlightBlock.f(div);

	}
}
