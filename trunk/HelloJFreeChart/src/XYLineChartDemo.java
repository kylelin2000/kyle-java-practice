import java.awt.Font;
import java.io.IOException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;


public class XYLineChartDemo {
	public static void main(String[] args) throws IOException {
		XYDataset dataset = getDataSet();
		JFreeChart chart = ChartFactory.createXYLineChart("水果產量圖", // 图表标题
				"月份", // 目录轴的显示标签
				"產量", // 数值轴的显示标签
				dataset, // 数据集
				PlotOrientation.VERTICAL, // 图表方向：水平、垂直
				true, // 是否显示图例(对于简单的柱状图必须是 false)
				false, // 是否生成工具
				false // 是否生成 URL 链接
				);
		
		Font font = new Font("標楷體",Font.PLAIN,12);
		chart.getTitle().setFont(font);
		chart.getLegend().setItemFont(font);
		ValueAxis domainAxis = chart.getXYPlot().getDomainAxis();
		domainAxis.setLabelFont(font);
		domainAxis.setTickLabelFont(font);
		ValueAxis rangeAxis = chart.getXYPlot().getRangeAxis();
		rangeAxis.setLabelFont(font);
		rangeAxis.setTickLabelFont(font);
		
		FileUtil.saveJpgFile(chart);
	}
	
	private static XYDataset getDataSet() {
		XYSeries xyseries = new XYSeries("蘋果"); // 先产生XYSeries 对象
		xyseries.add(1.0D, 1.0D);
		xyseries.add(2D, 4D);
		xyseries.add(3D, 3D);
		xyseries.add(4D, 5D);
		xyseries.add(5D, 5D);
		xyseries.add(6D, 7D);
		xyseries.add(7D, 7D);
		xyseries.add(8D, 8D);

		XYSeries xyseries1 = new XYSeries("梨子");
		xyseries1.add(1.0D, 5D);
		xyseries1.add(2D, 7D);
		xyseries1.add(3D, 6D);
		xyseries1.add(4D, 8D);
		xyseries1.add(5D, 4D);
		xyseries1.add(6D, 4D);
		xyseries1.add(7D, 2D);
		xyseries1.add(8D, 1.0D);

		XYSeries xyseries2 = new XYSeries("香蕉");
		xyseries2.add(3D, 4D);
		xyseries2.add(4D, 3D);
		xyseries2.add(5D, 2D);
		xyseries2.add(6D, 3D);
		xyseries2.add(7D, 6D);
		xyseries2.add(8D, 3D);
		xyseries2.add(9D, 4D);
		xyseries2.add(10D, 3D);

		XYSeriesCollection xyseriescollection = new XYSeriesCollection(); // 再用XYSeriesCollection添加入XYSeries
		xyseriescollection.addSeries(xyseries);
		xyseriescollection.addSeries(xyseries1);
		xyseriescollection.addSeries(xyseries2);
		return xyseriescollection;
	}
}
