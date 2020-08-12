package anomalydetector;

/**
 * Represents an anomaly at a specific point in time.
 */
public class PointAnomaly {
	/**
	 * The node and metric that anomaly is for
	 */
	private String node, metric;
	/**
	 * The time of the anomaly
	 */
	private long timestamp;
	/**
	 * The expected value (from egads) and actual value (from kafka) at that point
	 */
	private float expectedValue, actualValue;
	
	/**
	 * @param node the node the anomaly is from
	 * @param metric the metric in that node the anomaly is from
	 * @param timestamp the unix timestamp for the anomaly
	 * @param expectedValue the value that the model expected at that time
	 * @param actualValue the actual value from kafka at that time
	 */
	public PointAnomaly(String node, String metric, long timestamp, float expectedValue, float actualValue) {
		this.node = node;
		this.metric = metric;
		this.timestamp = timestamp;
		this.expectedValue = expectedValue;
		this.actualValue = actualValue;
	}
	
	public String getNode() {
		return node;
	}
	
	public String getMetric() {
		return metric;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public float getExpectedValue() {
		return expectedValue;
	}
	
	public float getActualValue() {
		return actualValue;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Anomaly at:    ");
		sb.append("Node: ").append(node).append(" - ");
		sb.append("Metric: ").append(metric).append(" - ");
		sb.append("Time: ").append(timestamp).append(" - ");
		sb.append("Expected: ").append(expectedValue).append(" - ");
		sb.append("Actual: ").append(actualValue);
		return sb.toString();
	}
}
