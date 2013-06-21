import java.awt.Color;
import java.awt.Font;
import java.io.IOException;
import java.text.NumberFormat;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.DialShape;
import org.jfree.chart.plot.MeterInterval;
import org.jfree.chart.plot.MeterPlot;
import org.jfree.data.Range;
import org.jfree.data.general.DefaultValueDataset;

public class MeterChartDemo {
	public static void main(String[] args) throws IOException {
		DefaultValueDataset data = new DefaultValueDataset(32.0);
		MeterPlot plot = new MeterPlot(data);
		plot.setDialShape(DialShape.CHORD);
		plot.setDialBackgroundPaint(Color.WHITE);
		plot.setRange(new Range(0, 120));
		plot.setDialOutlinePaint(Color.GRAY);
		plot.setNeedlePaint(Color.BLACK);
		plot.setTickLabelsVisible(true);
		plot.setTickLabelPaint(Color.BLACK);
		plot.setTickPaint(Color.GRAY);
		plot.setTickLabelFormat(NumberFormat.getNumberInstance());
		plot.setTickSize(10);
		plot.setValuePaint(Color.BLACK);
		plot.addInterval(new MeterInterval("Low", new Range(0, 70), null, null,
				new Color(128, 255, 128, 90)));
		plot.addInterval(new MeterInterval("Normal", new Range(70, 100), null,
				null, new Color(255, 255, 128, 90)));
		plot.addInterval(new MeterInterval("High", new Range(100, 120), null,
				null, new Color(255, 128, 128, 90)));

		// 创建chart，最后一个参数决定是否显示图例
		final JFreeChart chart = new JFreeChart("指針圖", JFreeChart.DEFAULT_TITLE_FONT, plot, false);
		Font font = new Font("標楷體",Font.PLAIN,12);
		chart.getTitle().setFont(font);
		
		FileUtil.saveJpgFile(chart);
	}
}
