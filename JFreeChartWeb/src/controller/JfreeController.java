package controller;

import java.awt.Font;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.servlet.ServletUtilities;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller  
@RequestMapping("/Jfree")  
public class JfreeController { 
	@RequestMapping("/showBar")  
    public ModelAndView showBar(HttpServletRequest request,  
            HttpServletResponse response) throws Exception {  
        response.setContentType("text/html;charset=UTF-8");  
        Writer out = response.getWriter();  
  
        PrintWriter pw = new PrintWriter(out);
        
        String filename1 = generateBarChart(pw);
		String filename2 = generateTimeSeriesChart(pw);
        
        String file1 = request.getContextPath()  + "/servlet/DisplayChart?filename=" + filename1;
        String file2 = request.getContextPath()  + "/servlet/DisplayChart?filename=" + filename2;
        ModelAndView mav = new ModelAndView();  
        mav.addObject("file1", file1); 
        mav.addObject("file2", file2);
        mav.setViewName("/chart");  
        return mav;  
    }

	private String generateTimeSeriesChart(PrintWriter pw) throws IOException {
		JFreeChart chart = ChartFactory.createTimeSeriesChart(
				"水果產量圖", // title
				"月份", // x-axis label
				"水果", // y-axis label
				getXYDataset(), // data
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
		
		String filename = ServletUtilities.saveChartAsPNG(chart, 400, 300, null);
		StandardEntityCollection sec = new StandardEntityCollection();  
        ChartRenderingInfo info = new ChartRenderingInfo(sec); 
        ChartUtilities.writeImageMap(pw, "imgMap", info, false);
        
		return filename;
	}

	private String generateBarChart(PrintWriter pw) throws Exception {
		JFreeChart chart = ChartFactory.createBarChart3D("水果產量圖", // 图表标题
				"水果", // 目录轴的显示标签
				"產量", // 数值轴的显示标签
				getCategoryBarDataset(), // 数据集
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
		
		String filename = ServletUtilities.saveChartAsPNG(chart, 400, 300, null);
		StandardEntityCollection sec = new StandardEntityCollection();  
        ChartRenderingInfo info = new ChartRenderingInfo(sec); 
        ChartUtilities.writeImageMap(pw, "imgMap", info, false);
        
        return filename;
	}
	
	private static CategoryDataset getCategoryBarDataset() {
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
	
	private static XYDataset getXYDataset() {
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
