package anomalydetector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * Structure that stores detected anomalies sorted by node and metric.
 */
public class AnomalyDatabase {
	
	/**
	 * Collection of all anomalies organized by node and metric.
	 * First key is derived from the OpenNMS node the metric is from, second key is the name of the metric.
	 */
	private LinkedHashMap<String, LinkedHashMap<String, List<PointAnomaly>>> anomalies;
	
	/**
	 * Collection mapping the number of anomalies by rough timestamp.
	 */
	private LinkedHashMap<Double, Double> numAnomaliesByTime;
	
	/**
	 * The value that timestamps in the anomalyByTime collection are rounded to
	 * (ie: timestamps are rounded to multiples of this value)
	 */
	private static int TIME_COARSENESS = 10000;
	
	/**
	 * Create a new empty anomaly database object
	 */
	public AnomalyDatabase() {
		anomalies = new LinkedHashMap<String, LinkedHashMap<String, List<PointAnomaly>>>();
    	numAnomaliesByTime = new LinkedHashMap<Double, Double>();
	}
	
	/**
	 * Adds a new anomaly to the database.
	 * If an anomaly already exists for the new anomaly's timestamp, the old anomaly is overwritten.
	 * @param anomaly the anomaly to be added
	 */
	public void AddAnomaly(PointAnomaly anomaly) {
		String nodeKey = anomaly.getNode();
		String metricKey = anomaly.getMetric();
		if(!anomalies.containsKey(nodeKey)) {
			anomalies.put(nodeKey, new LinkedHashMap<String, List<PointAnomaly>>());
		}
		if(!anomalies.get(nodeKey).containsKey(metricKey)) {
			anomalies.get(nodeKey).put(metricKey, new LinkedList<PointAnomaly>());
		}
		
		List<PointAnomaly> anomalyList = anomalies.get(nodeKey).get(metricKey);
		//check if an anomaly already exists at this timestamp for this node & metric
		for(int i = 0; i < anomalyList.size(); i++) {
			if(anomalyList.get(i).getTimestamp() == anomaly.getTimestamp()) {
				anomalyList.remove(i);
				anomalyList.add(i, anomaly); //swap the old for new; newly detected anomaly has a more recent expected value from the model
				return;
			}
		}
		
		//add the anomaly if there does not already exist an anomaly for this node + metric at the given timestamp
		//also add that an anomaly was detected to the numAnomaliesByTime collection
		double roundedTimestamp = anomaly.getTimestamp() - (anomaly.getTimestamp() % TIME_COARSENESS);
    	if(numAnomaliesByTime.containsKey(roundedTimestamp)) {
    		numAnomaliesByTime.put(roundedTimestamp, numAnomaliesByTime.get(roundedTimestamp)+1);
    	} else {
    		numAnomaliesByTime.put(roundedTimestamp, 1D);
    	}
        
		anomalyList.add(anomaly);
	}
	
	public List<Double> GetNumAnomaliesByTimeTimestamps() {
		return Lists.newArrayList(numAnomaliesByTime.keySet());
	}
	
	public List<Double> GetNumAnomaliesByTimeValues() {
		List<Double> numAnomalies = new LinkedList<Double>();
		for(Double key : numAnomaliesByTime.keySet()) {
			numAnomalies.add(numAnomaliesByTime.get(key));
		}
		return numAnomalies;
	}
	
	/**
	 * @return a list of all anomalies from all nodes & metrics
	 */
	public List<PointAnomaly> GetAllAnomalies() {
		List<PointAnomaly> anomList = new LinkedList<PointAnomaly>();
		for(String nodeKey : anomalies.keySet()) {
			for(String metricKey : anomalies.get(nodeKey).keySet()) {
				for(PointAnomaly anomaly : anomalies.get(nodeKey).get(metricKey)) {
					anomList.add(anomaly);
				}
			}
		}
		return anomList;
	}
	
	/**
	 * @param node the key for the node to get anomalies from
	 * @return a list of all anomalies from a given node
	 */
	public List<PointAnomaly> GetAnomalies(String node) {
		List<PointAnomaly> anomList = new LinkedList<PointAnomaly>();
		for(String metricKey : anomalies.get(node).keySet()) {
			for(PointAnomaly anomaly : anomalies.get(node).get(metricKey)) {
				anomList.add(anomaly);
			}
		}
		return anomList;
	}
	
	/**
	 * @param node the key for the node to get anomalies from
	 * @param metric the key for the metric to get anomalies from
	 * @return a list of all anomalies for a given node's metric
	 */
	public List<PointAnomaly> GetAnomalies(String node, String metric) {
		List<PointAnomaly> anomList = new LinkedList<PointAnomaly>();
		for(PointAnomaly anomaly : anomalies.get(node).get(metric)) {
			anomList.add(anomaly);
		}
		return anomList;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ANOMALY DATABASE\n");
		for(String nodeKey : anomalies.keySet()) {
			sb.append("---------------------------");
			sb.append("NODE: " + nodeKey + "\n");
			for(String metricKey : anomalies.get(nodeKey).keySet()) {
				sb.append("METRIC: " + metricKey + "\n");
				sb.append(anomalies.get(nodeKey).get(metricKey) + "\n");
			}
		}
		sb.append("----------------------------");
		return sb.toString();
	}
}
