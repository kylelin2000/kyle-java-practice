import java.awt.BasicStroke;
import java.awt.Color;
import java.io.IOException;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.ThermometerPlot;
import org.jfree.data.general.DefaultValueDataset;


public class ThermometerChartDemo {
	public static void main(String[] args) throws IOException {
		final DefaultValueDataset dataset = new DefaultValueDataset(new Double(
				43.0));

		// create the chart...
		final ThermometerPlot plot = new ThermometerPlot(dataset);
		final JFreeChart chart = new JFreeChart("溫度計圖", // chart title
				JFreeChart.DEFAULT_TITLE_FONT, plot, // plot
				false); // include legend

		plot.setThermometerStroke(new BasicStroke(2.0f));
		plot.setThermometerPaint(Color.lightGray);

		FileUtil.saveJpgFile(chart);
	}
}
