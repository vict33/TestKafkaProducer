package vl.tmp.kafka.TestKafkaProducer;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DocProducer {
	// private final String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String TOPIC = "vlTest01";
	private static Producer<Integer, String> producer;

	public static void main(String[] args) {
		// Class KafkaProducer<K,V>
		// https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
		// Class KafkaConsumer<K,V>
		// https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.80.51:9092");
		// props.put("acks", "all");
		// props.put("retries", 0);
		// props.put("batch.size", 16384);
		// props.put("linger.ms", 1);
		// props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// Async
		producer = new KafkaProducer<>(props);
		System.out.println("Producer-async-create: " + new Date());
		for (int i = 0; i < 2; i++) {
			long startTime = System.currentTimeMillis();
			producer.send(new ProducerRecord<Integer, String>(TOPIC, i, "qq-" + Long.toString(startTime)));
		}
		producer.close();
		System.out.println("Producer-async-close: " + new Date());

		// Sync
		producer = new KafkaProducer<>(props);
		System.out.println("Producer-sync-create: " + new Date());
		for (int i = 0; i < 2; i++) {
			long startTime = System.currentTimeMillis();
			producer.send(new ProducerRecord<Integer, String>(TOPIC, i, "as-" + Long.toString(startTime)), new Callback() {
				@Override
				public void onCompletion(RecordMetadata recMetadata, Exception e) {
					System.out.println("Sent data to Offset " + recMetadata.offset() + " in Partition " + recMetadata.partition());
				}
			});
		}
		producer.close();
		System.out.println("Producer-sync-close: " + new Date());
	}
}
