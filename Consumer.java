package testartifact;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
	
	public static void adminTest() 
	{
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("connections.max.idle.ms", 10000);
		properties.put("request.timeout.ms", 5000);
		try
		{
			AdminClient client = KafkaAdminClient.create(properties);
		    ListTopicsResult topics = client.listTopics();
		    Set<String> names = topics.names().get();
		    for(String name : names) {
		    	System.out.println(name);
		    }
		    client.close();
		}
		catch (Exception e)
		{
			System.out.println("exception");
		}
	}
	
	public static void consumerTest() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "testid");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		List<String> topics = Arrays.asList("alarms", "metrics", "nodes", "events");
		consumer.subscribe(topics);
		
		try {
			while(true) {
				System.out.println("polling");
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));
				for(ConsumerRecord<String, String> record : records) {
					System.out.printf("topic =%s, partiation = %s, offset = %d, key = %s, value = %s\n",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
				}
			}
		} finally {
			consumer.close();
		}
	}
	
	public static void main(String[] args) {
		adminTest();
		consumerTest();
		
		/**
		 *  nodes
			test
			alarms
			metrics
			events
			---
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
			polling
		 * 
		 * 
		 */
	}

}
