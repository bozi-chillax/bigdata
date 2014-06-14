package com.chillax.kafka;

import java.util.Properties;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.identifier_return;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.producer.SyncProducer;

public class ProducerSample {

	public static void main(String[] args) {

		Properties props = new Properties();
		// props.put("zookeeper.connect", "rdp.chinamobiad.com:12181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("metadata.broker.list", "rdp.chinamobiad.com:19092");
		props.put("producer.type", "sync");
		props.put("metadata.broker.list", "192.168.0.127:9092");

		ProducerConfig config = new ProducerConfig(props);
		Producer<Integer, String> producer = new Producer<Integer, String>(
				config);
		int i = 0;
		while (i < 100) {
			KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(
					"zzcm", "wozaizuoceshi" + i);
			producer.send(data);
			i++;
		}
		producer.close();

	}

}
