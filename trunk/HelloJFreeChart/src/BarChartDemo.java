
import java.awt.Font;
import java.io.IOException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

public class BarChartDemo {
	public static void main(String[] args) throws IOException {
		CategoryDataset dataset = getDataSet2();
		JFreeChart chart = ChartFactory.createBarChart3D("水果產量圖", // 图表标题
				"水果", // 目录轴的显示标签
				"產量", // 数值轴的显示标签
				dataset, // 数据集
				PlotOrientation.HORIZONTAL, // 图表方向：水平、垂直
				true, // 是否显示图例(对于简单的柱状图必须是 false)
				false, // 是否生成工具
				false // 是否生成 URL 链接
				);

		Font font = new Font("標楷體",Font.PLAIN,12);
		chart.getTitle().setFont(font);
		chart.getLegend().setItemFont(font);
		CategoryAxis domainAxis = chart.getCategoryPlot().getDomainAxis();
		domainAxis.setLabelFont(font);
		domainAxis.setTickLabelFont(font);
		ValueAxis rangeAxis = chart.getCategoryPlot().getRangeAxis();
		rangeAxis.setLabelFont(font);
		rangeAxis.setTickLabelFont(font);
		
		FileUtil.saveJpgFile(chart);
	}

	private static CategoryDataset getDataSet() {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		dataset.addValue(100, null, "蘋果");
		dataset.addValue(200, null, "梨子");
		dataset.addValue(300, null, "葡萄");
		dataset.addValue(400, null, "香蕉");
		dataset.addValue(500, null, "荔枝");
		return dataset;
	}

	private static CategoryDataset getDataSet2() {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		dataset.addValue(110, "北京", "蘋果");
		dataset.addValue(120, "上海", "蘋果");
		dataset.addValue(100, "廣州", "蘋果");
		dataset.addValue(210, "北京", "梨子");
		dataset.addValue(220, "上海", "梨子");
		dataset.addValue(200, "廣州", "梨子");
		dataset.addValue(310, "北京", "葡萄");
		dataset.addValue(320, "上海", "葡萄");
		dataset.addValue(300, "廣州", "葡萄");
		dataset.addValue(410, "北京", "香蕉");
		dataset.addValue(420, "上海", "香蕉");
		dataset.addValue(400, "廣州", "香蕉");
		dataset.addValue(510, "北京", "荔枝");
		dataset.addValue(520, "上海", "荔枝");
		dataset.addValue(500, "廣州", "荔枝");
		return dataset;
	}
}