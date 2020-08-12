package anomalydetector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import org.opennms.features.kafka.producer.model.CollectionSetProtos.NodeLevelResource;
import org.opennms.features.kafka.producer.model.CollectionSetProtos.NumericAttribute;

/**
 * Data structure that stores individual timeseries for each metric obtained through kafka
 * As new data is added, older datapoints are automatically removed if the difference between their timestamps
 * is larger than the designated windowSize (in milliseconds)
 * Internally uses hashmaps for efficient lookup of existing timeseries as new datapoints are added from kafka
 */
public class MetricsDatabase {
	/**
	 * Metrics stored in timeseries form. The first key is derived from the opennms node, the second is by the specific metric
	 * being measured for that node.
	 */
	private LinkedHashMap<String, LinkedHashMap<String, MetricTimeSeries>> metrics;
	/**
	 * Time in Ms that represents the largest possible time in milliseconds between the oldest and newest datapoint in any timeseries at any point in time
	 * As new data is added, this value is used to remove older datapoints
	 */
	private long windowSize;
	
	/**
	 * Creates a new (empty) metrics database
	 * @param windowSize the longest allowed time in milliseconds between datapoints of any metric
	 */
	public MetricsDatabase(long windowSize) {
		this.metrics = new LinkedHashMap<String, LinkedHashMap<String, MetricTimeSeries>>();
		this.windowSize = windowSize;
	}
	
	/**
	 * Adds a datapoint into one of the timeseries stored in this class.
	 * Also removes old datapoints if the new datapoint would cause them to fall outside of the window.
	 * @param timestamp unix time of the new datapoint
	 * @param value value of the datapoint
	 * @param sourceNode source node in OpenNMS that the data comes from
	 * @param numAttribute numeric attribute from OpenNMS from the source node
	 */
	public void addData(long timestamp, float value, NodeLevelResource sourceNode, NumericAttribute numAttribute) {
		String nodeKey = sourceNode.getNodeId() + "_" + sourceNode.getForeignSource();
		String metricKey = numAttribute.getGroup() + "_" + numAttribute.getName();
		if(!metrics.containsKey(nodeKey)) {
			System.out.println("New node discovered: " + nodeKey);
			metrics.put(nodeKey, new LinkedHashMap<String, MetricTimeSeries>());
		}
		if(!metrics.get(nodeKey).containsKey(metricKey)) {
			System.out.println("New metric " + metricKey + " added to node " + nodeKey);
			metrics.get(nodeKey).put(metricKey, new MetricTimeSeries(nodeKey, metricKey));
		}
		
		MetricTimeSeries metricTS = metrics.get(nodeKey).get(metricKey);
		//add the metric's data to relevant timeseries
		metricTS.AddData(timestamp, (float)numAttribute.getValue());
		
		while(timestamp - metricTS.getTimestamp(0) > windowSize) { //while time difference between new entry and oldest datapoint is > window size
			metricTS.removeDatapoint(0); //remove oldest datapoint
		}
	}
	
	/**
	 * @return list of timeseries reprsenting the metrics stored in this structure
	 */
	public List<MetricTimeSeries> asList() {
		LinkedList<MetricTimeSeries> list = new LinkedList<MetricTimeSeries>();
		for(String nodeKey : metrics.keySet()) {
			for(String metricKey : metrics.get(nodeKey).keySet()) {
				list.add(metrics.get(nodeKey).get(metricKey));
			}
		}
		return list;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("METRICS");
		for(String nodeKey : metrics.keySet()) {
			sb.append("---------------------------");
			sb.append("NODE: " + nodeKey + "\n");
			for(String metricKey : metrics.get(nodeKey).keySet()) {
				sb.append("METRIC: " + metrics.get(nodeKey).get(metricKey).getMetricName() + "\n");
				sb.append(metrics.get(nodeKey).get(metricKey) + "\n");
			}
		}
		sb.append("----------------------------");
		return sb.toString();
	}
}
