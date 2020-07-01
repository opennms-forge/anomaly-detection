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
		properties.put("bootstrap.servers", "172.20.42.84:9092");
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
		props.put("bootstrap.servers", "172.20.42.84:9092");
		props.put("group.id", "testid");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		List<String> topics = Arrays.asList("OpenNMS.Alarms");
		consumer.subscribe(topics);
		
		try {
			for(String topicName : consumer.listTopics().keySet()) {
				System.out.println(topicName + "   ");
			}
			
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
		//adminTest();
		consumerTest();
		
		/*
		    connect-configs
			OpenNMS.Sink.Trap NO
			none
			OpenNMS.BeachHouse.rpc-request.PING-SWEEP NO
			OpenNMS.BeachHouse.rpc-request.Collect YES
			OpenNMS.Ambergate.rpc-request.DNS YES
			OpenNMS.Sink.Telemetry-IPFIX NO
			OpenNMS.Sink.Telemetry-SFlow NO
			OpenNMS.rpc-response.Echo YES
			OpenNMS.Sink.Events YES (looks like events xml?)
			OpenNMS.Apex.rpc-request.SNMP YES
			OpenNMS.Apex.rpc-request.Echo YES
			OpenNMS.Ambergate.rpc-request.Collect YES
			OpenNMS.Topology.Vertices NO
			events
			OpenNMS.Ambergate.rpc-request.Requisition NO
			test
			OpenNMS.rpc-response.Detect YES
			OpenNMS.rpc-response.PING NO
			OpenNMS.Alarm.Feedback NO
			OpenNMS.Ambergate.rpc-request.SNMP YES
			OpenNMS.BeachHouse.rpc-request.Detect YES
			OpenNMS.rpc-response.Poller YES, has time?
			OpenNMS.BeachHouse.rpc-request.PING NO
			OpenNMS.Sink.Heartbeat YES
			OpenNMS.Apex.rpc-request.Requisition NO
			metrics
			OpenNMS.Topology.Edges NO
			OpenNMS.Apex.rpc-request.PING-SWEEP NO
			OpenNMS.Ambergate.rpc-request.PING-SWEEP NO
			OpenNMS.Nodes NO
			OpenNMS.Apex.rpc-request.Poller YES
			OpenNMS.Sink.Telemetry-Netflow-9 YES, slows computer
			OpenNMS.Apex.rpc-request.Detect YES
			OpenNMS.Ambergate.rpc-request.Detect YES
			OpenNMS.BeachHouse.rpc-request.Requisition NO
			OpenNMS.Ambergate.rpc-request.Echo YES
			OpenNMS.Sink.Telemetry-Netflow-5 NO
			OpenNMS.BeachHouse.rpc-request.DNS YES
			OpenNMS.Ambergate.rpc-request.Poller YES
			OpenNMS.Apex.rpc-request.PING NO
			OpenNMS.Sink.Syslog NO
			OpenNMS.Ambergate.rpc-request.PING NO
			OpenNMS.Apex.rpc-request.DNS NO
			OpenNMS.BeachHouse.rpc-request.Poller YES
			OpenNMS.BeachHouse.rpc-request.SNMP YES
			alarms
			OpenNMS.BeachHouse.rpc-request.Echo YES
			OpenNMS.rpc-response.SNMP YES
			OpenNMS.rpc-response.DNS YES
			nodes
			OpenNMS.rpc-response.Collect YES
			OpenNMS.Apex.rpc-request.Collect YES
			OpenNMS.Alarms NO
		 */
	}

}
