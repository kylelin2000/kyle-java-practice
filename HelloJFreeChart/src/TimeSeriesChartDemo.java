import java.awt.Font;
import java.io.IOException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;

public class TimeSeriesChartDemo {
	public static void main(String[] args) throws IOException {
		XYDataset dataset = createDataset();
		JFreeChart chart = ChartFactory.createTimeSeriesChart(
				"水果產量圖", // title
				"月份", // x-axis label
				"水果", // y-axis label
				dataset, // data
				true, // create legend?
				true, // generate tooltips?
				false // generate URLs?
				);

		Font font = new Font("標楷體", Font.PLAIN, 12);
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

	private static XYDataset createDataset() {
		TimeSeries s1 = new TimeSeries("蘋果");
		s1.add(new Month(2, 2001), 181.8);
		s1.add(new Month(3, 2001), 167.3);
		s1.add(new Month(4, 2001), 153.8);
		s1.add(new Month(5, 2001), 167.6);
		s1.add(new Month(6, 2001), 158.8);
		s1.add(new Month(7, 2001), 148.3);
		s1.add(new Month(8, 2001), 153.9);
		s1.add(new Month(9, 2001), 142.7);
		s1.add(new Month(10, 2001), 123.2);
		s1.add(new Month(11, 2001), 131.8);
		s1.add(new Month(12, 2001), 139.6);
		s1.add(new Month(1, 2002), 142.9);
		s1.add(new Month(2, 2002), 138.7);
		s1.add(new Month(3, 2002), 137.3);
		s1.add(new Month(4, 2002), 143.9);
		s1.add(new Month(5, 2002), 139.8);
		s1.add(new Month(6, 2002), 137.0);
		s1.add(new Month(7, 2002), 132.8);

		TimeSeries s2 = new TimeSeries("梨子");
		s2.add(new Month(2, 2001), 129.6);
		s2.add(new Month(3, 2001), 123.2);
		s2.add(new Month(4, 2001), 117.2);
		s2.add(new Month(5, 2001), 124.1);
		s2.add(new Month(6, 2001), 122.6);
		s2.add(new Month(7, 2001), 119.2);
		s2.add(new Month(8, 2001), 116.5);
		s2.add(new Month(9, 2001), 112.7);
		s2.add(new Month(10, 2001), 101.5);
		s2.add(new Month(11, 2001), 106.1);
		s2.add(new Month(12, 2001), 110.3);
		s2.add(new Month(1, 2002), 111.7);

		TimeSeriesCollection dataset = new TimeSeriesCollection();
		dataset.addSeries(s1);
		dataset.addSeries(s2);

		return dataset;
	}
}
