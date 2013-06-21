import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;


public class FileUtil {
	public static void saveJpgFile(JFreeChart chart) throws FileNotFoundException, IOException {
		FileOutputStream fos_jpg = null;
		try {
			fos_jpg = new FileOutputStream("D:\\fruit.jpg");
			ChartUtilities.writeChartAsJPEG(fos_jpg, 1.0f, chart, 400, 300,
					null);
		} finally {
			try {
				fos_jpg.close();
			} catch (Exception e) {
			}
		}
	}
}
