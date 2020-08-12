package anomalydetector;

import java.util.List;

import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler.LegendPosition;

import com.google.common.primitives.Doubles;

/***
 * Manages and draws a simple line graph to the screen showing the amount of detected anomalies over time.
 * Drawn and updated in the update method.
 */
public class GraphUI {
	
	SwingWrapper<CategoryChart> wrapper;
	
	public GraphUI() {
		
	}
	
	/**
	 * Updates the chart's x and y-axis data. Repaints the updated chart.
	 * Initializes the chart the first time it is called with nonempty lists.
	 * @param timeData the x-axis of the chart; rough timestamps. Size must be same for both parameter lists.
	 * @param anomalyCountData the y-axis of the chart; number of anomalies in that timestamp
	 */
	public void update(List<Double> timeData, List<Double> anomalyCountData) {
		//Just exit if the data lists are empty or incorrectly sized
		if(timeData.size() == 0 || timeData.size() != anomalyCountData.size()) {
			return;
		}
		else if(wrapper == null) { //first time setup
			CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Anomalies over Time").xAxisTitle("Timestamp").yAxisTitle("Anomaly Count").build();
			
			chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
			chart.getStyler().setHasAnnotations(true);
			
			chart.addSeries("series", Doubles.toArray(timeData), Doubles.toArray(anomalyCountData));
			
			wrapper = new SwingWrapper<CategoryChart>(chart);
			
			wrapper.displayChart();
		} else { //update with new data
			wrapper.getXChartPanel().getChart().updateCategorySeries("series", Doubles.toArray(timeData), Doubles.toArray(anomalyCountData), null);
			wrapper.repaintChart(); //redraw
		}
	}
}
