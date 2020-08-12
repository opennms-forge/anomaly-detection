package anomalydetector;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.opennms.features.kafka.producer.model.CollectionSetProtos.NodeLevelResource;

/**
 * Data structure for a series of time-value pairings. 
 * Also stores the source node in OpenNMS and name of the metric that data comes from.
 */
public class MetricTimeSeries {
	private class DataPoint {
		public long timestamp; 
		public float value;
		
		public DataPoint(long timestamp, float value) {
			this.timestamp = timestamp;
			this.value = value;
		}
		
		@Override
		public String toString() {
			return "{" + new Date(timestamp).toString() + ", " + value + "}";
		}
	}

	/***
	 * The node in OpenNMS where the data originated from
	 */
	private String sourceNode;
	
	/***
	 * The specific metric on that node this data represents
	 */
	private String metricName;
	private List<DataPoint> data;
	
	/**
	 * Creates a new empty timeseries
	 * @param nodeID the id of the node the data originated from
	 * @param metricName a string describing the data being stored
	 */
	public MetricTimeSeries(String sourceNode, String metricName) {
		this.sourceNode = sourceNode;
		this.metricName = metricName;
		this.data = new LinkedList<DataPoint>();
	}
	
	/**
	 * Adds a new datapoint to the time series, maintaining internal order by timestamp. 
	 * If a new datapoint has the same timestamp as an older datapoint, the older datapoint is overwritten. 
	 * 
	 * @param timestamp the unix time in milliseconds
	 * @param d the value at that time
	 */
	public void AddData(long timestamp, float d) {
		DataPoint toAdd = new DataPoint(timestamp, d);
		if(data.size() == 0) {
			data.add(toAdd);
			return;
		}
		//iterate over existing datapoints to place this one chronologically in the right place
		for(int i = 0; i < data.size(); i++) {
			if(toAdd.timestamp <= data.get(i).timestamp) {
				if(toAdd.timestamp == data.get(i).timestamp) {
					//System.out.println("Warning: shared timestamp, overwriting existing datapoint");
					data.remove(i);
				}
				data.add(i, toAdd);
				return;
			}
		}
		//if no point in the data had a timestamp smaller than this point, this is the most recent data and added at end
		data.add(toAdd);
	}

	/***
	 * @return the name of the metric this timeseries stores data for
	 */
	public String getMetricName() {
		return metricName;
	}
	
	/***
	 * @return the number of entries in this timeseries
	 */
	public int size() {
		return data.size();
	}
	
	public double getValue(int index) {
		return data.get(index).value;
	}
	
	public long getTimestamp(int index) {
		return data.get(index).timestamp;
	}
	
	public void removeDatapoint(int index) {
		data.remove(index);
	}
	
	/***
	 * @return the OpenNMS node this timeseries came from
	 */
	public String getSourceNode() {
		return sourceNode;
	}
	
	/**
	 * @return a list of all timestamps in this timeseries
	 */
	public List<Long> getTimestamps() {
		List<Long> timestamps = new ArrayList<Long>();
		for(DataPoint d : data) {
			timestamps.add(d.timestamp);
		}
		return timestamps;
	}
	
	/**
	 * @return a list of all values in this timeseries
	 */
	public List<Float> getValues() {
		List<Float> values = new ArrayList<Float>();
		for(DataPoint d : data) {
			values.add(d.value);
		}
		return values;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("sourceNode:\n").append(sourceNode.toString());
		sb.append("metric: ").append(metricName).append("\n");
		for(DataPoint d : data) {
			sb.append(d.toString()).append("\n");
		}
		return sb.toString();
	}
}
