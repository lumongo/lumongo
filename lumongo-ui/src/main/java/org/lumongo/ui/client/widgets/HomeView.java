package org.lumongo.ui.client.widgets;

import com.google.gwt.core.client.Scheduler;
import gwt.material.design.client.ui.MaterialCard;
import gwt.material.design.client.ui.MaterialCardContent;
import gwt.material.design.client.ui.MaterialCardTitle;
import gwt.material.design.client.ui.MaterialColumn;
import gwt.material.design.client.ui.MaterialLabel;
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

		MaterialRow row = new MaterialRow();
		add(row);

		{
			MaterialColumn infoColumn = new MaterialColumn();
			infoColumn.setGrid("s12 m6");
			row.add(infoColumn);

			MaterialCard infoCard = new MaterialCard();
			MaterialCardContent infoContent = new MaterialCardContent();
			infoCard.add(infoContent);
			infoColumn.add(infoCard);

			MaterialCardTitle infoCardTitle = new MaterialCardTitle();
			infoCardTitle.setText("Basic Info");
			infoContent.add(infoCardTitle);

			MaterialLabel lumongoVersionLabel = new MaterialLabel("LuMongo Version: " + instanceInfo.getLumongoVersion());
			MaterialLabel luceneVersionLabel = new MaterialLabel("Lucene Version: " + instanceInfo.getLuceneVersion());
			MaterialLabel lumongoMemoryLabel = new MaterialLabel("LuMongo Memory: " + instanceInfo.getLumongoMemory());
			MaterialLabel jvmFreeMemoryLabel = new MaterialLabel("JVM Free Memory: " + instanceInfo.getJvmFreeMemory());
			MaterialLabel jvmMaxMemoryLabel = new MaterialLabel("JVM Max Memory: " + instanceInfo.getJvmMaxMemoryMB());
			MaterialLabel jvmTotalMemoryLabel = new MaterialLabel("JVM Total Memory: " + instanceInfo.getJvmTotalMemoryMB());
			MaterialLabel jvmUsedMemoryLabel = new MaterialLabel("JVM Used Memory: " + instanceInfo.getJvmUsedMemory());

			infoContent.add(lumongoVersionLabel);
			infoContent.add(luceneVersionLabel);
			infoContent.add(lumongoMemoryLabel);
			infoContent.add(jvmFreeMemoryLabel);
			infoContent.add(jvmMaxMemoryLabel);
			infoContent.add(jvmTotalMemoryLabel);
			infoContent.add(jvmUsedMemoryLabel);
		}

		{
			MaterialColumn chartColumn = new MaterialColumn(12, 6, 6);
			MaterialCard card = new MaterialCard();

			Map<String, Serializable> data = new HashMap<>();
			for (IndexInfo indexInfo : instanceInfo.getIndexes()) {
				data.put(indexInfo.getName(), indexInfo.getTotalDocs());
			}
			Scheduler.get().scheduleDeferred(() -> {
				Highcharts chart = PieChart.getBuilder().setChartTitle("Index Info").setHeight(400).setData(data).setYAxisAllowDecimals(false).build();
				card.add(chart);
				chartColumn.add(card);
				row.add(chartColumn);
			});

		}

	}
}
