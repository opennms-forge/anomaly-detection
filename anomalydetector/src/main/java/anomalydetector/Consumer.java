package anomalydetector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opennms.features.kafka.producer.model.CollectionSetProtos.CollectionSet;
import org.opennms.features.kafka.producer.model.CollectionSetProtos.CollectionSetResource;
import org.opennms.features.kafka.producer.model.CollectionSetProtos.NodeLevelResource;
import org.opennms.features.kafka.producer.model.CollectionSetProtos.NumericAttribute;

import com.google.protobuf.InvalidProtocolBufferException;

public class Consumer {
	private long pollingIntervalMs;
	private KafkaConsumer<String, byte[]> consumer;
	
	public Consumer(String bootstrap_servers, long pollingIntervalMs) {
		this.pollingIntervalMs = pollingIntervalMs;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrap_servers);
		props.put("group.id", "anomaly");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
		this.consumer = new KafkaConsumer<String, byte[]>(props);
		List<String> topics = Arrays.asList("metrics");
		consumer.subscribe(topics);
	}
	
	/**
	 * Gets raw data from Kafka cluster
	 * @return list of polled data from cluster
	 */
	private List<CollectionSet> consumeDataFromKafka() {
		List<CollectionSet> metrics = new ArrayList<CollectionSet>(); 
		long startTime = System.currentTimeMillis();
		
		try {
			System.out.println("Starting consumer...");
			while(System.currentTimeMillis() - startTime < pollingIntervalMs) {
				System.out.println("polling... (" + metrics.size() + ")");
				
				ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1L));
				
				for(ConsumerRecord<String, byte[]> record : records) {
					try {
						metrics.add(CollectionSet.parseFrom(record.value()));
					} catch (InvalidProtocolBufferException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			System.out.print(e);
		}
		
		return metrics;
	}
	
	/**
	 * Formats the raw data from kafka into a collection of timeseries. 
	 * Each timeseries in the returned list represents a single metric from one source node
	 * 
	 * @param kafkaOutput the raw data from kafka
	 * @return a list of timeseries, each entry of the list represents the timeseries of one metric and the associated source node
	 */
	private void addKafkaOutputToMetrics(MetricsDatabase metrics, List<CollectionSet> kafkaOutput) {
		//iterating over all of the polled data from kafka
		for(CollectionSet set : kafkaOutput) {
			//each collectionset from kafka has a single timestamp...
			long timestamp = set.getTimestamp();
			
			for(CollectionSetResource resource : set.getResourceList()) {
				//get the node associated with this kafka record
				NodeLevelResource sourceNode = null;
				if(resource.hasNode()) {
					sourceNode = resource.getNode();
				} else if (resource.hasInterface()) {
					sourceNode = resource.getInterface().getNode();
				} else if (resource.hasGeneric()) {
					sourceNode = resource.getGeneric().getNode();
				} else {
					//response resource?
					continue;
				}
				
				//iterate over each metric for this node...
				for(NumericAttribute numAttribute : resource.getNumericList()) {
					metrics.addData(timestamp, (float)numAttribute.getValue(), sourceNode, numAttribute);
				}
			}
		}
	}
	
	public void updateMetricsFromKafka(MetricsDatabase metrics) {
		addKafkaOutputToMetrics(metrics, consumeDataFromKafka());
	}
}
