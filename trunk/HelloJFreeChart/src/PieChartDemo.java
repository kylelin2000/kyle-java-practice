import java.awt.Font;
import java.io.IOException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;

public class PieChartDemo {
	public static void main(String[] args) throws IOException {
		DefaultPieDataset data = getDataSet();
		JFreeChart chart = ChartFactory.createPieChart3D("水果產量圖", // 图表标题
				data, true, // 是否显示图例
				false, false);
		// 写图表对象到文件，参照柱状图生成源码
		
		Font font = new Font("標楷體",Font.PLAIN,12);
		chart.getTitle().setFont(font);
		chart.getLegend().setItemFont(font);
		((PiePlot)chart.getPlot()).setLabelFont(font);
		
		FileUtil.saveJpgFile(chart);
	}

	private static DefaultPieDataset getDataSet() {
		DefaultPieDataset dataset = new DefaultPieDataset();
		dataset.setValue("蘋果", 100);
		dataset.setValue("梨子", 200);
		dataset.setValue("葡萄", 300);
		dataset.setValue("香蕉", 400);
		dataset.setValue("荔枝", 500);
		return dataset;
	}
}