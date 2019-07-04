package vl.tmp.kafka.TestKafkaProducer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestProducer extends Thread {
	// Java Code Examples for org.apache.kafka.clients.producer.Callback
	// https://www.programcreek.com/java-api-examples/index.php?api=org.apache.kafka.clients.producer.Callback

	// private final String BOOTSTRAP_SERVERS = "localhost:9092";
	private final String TOPIC = "vlTest01";
	private KafkaProducer<Integer, String> kafkaProducer;
	private final Boolean isAsync = true;

	public TestProducer() {
		Properties kafkaServerProps = new Properties();
		kafkaServerProps.put("bootstrap.servers", "192.168.80.51:9092");
		kafkaServerProps.put("max.block.ms", "5000");
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerProps.getProperty("bootstrap.servers"));
		producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaServerProps.getProperty("max.block.ms"));
		//producerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "DBToKafka");
		//producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "DBToKafka");
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		// producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
		kafkaProducer = new KafkaProducer<Integer, String>(producerProps);
		System.out.println("create-Producer: " + new Date());
	}

	@Override
	public void run() {
		Integer key = 11;
		String value = "";
		ProducerRecord<Integer, String> record;
		long startTime = System.currentTimeMillis();
		if (isAsync) {
			try {
				value = "qwerty11" + "|" + new Date();
				record = new ProducerRecord<Integer, String>(TOPIC, key, value);
				kafkaProducer.send(record).get();
				value = "qwerty22" + "|" + new Date();
				record = new ProducerRecord<Integer, String>(TOPIC, key, value);
				kafkaProducer.send(record).get();
				System.out.println("send-Async");
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		} else {
			value = "qwerty33" + "|" + new Date();
			record = new ProducerRecord<Integer, String>(TOPIC, key, value);
			kafkaProducer.send(record, new TestProducerCallback(key, value, startTime));
			System.out.println("send-Sync");
			//
			//kafkaProducer.send(new ProducerRecord<Integer, String>(TOPIC, key, value), new Callback() {
			//    @Override
			//    public void onCompletion(RecordMetadata recMetadata, Exception e) {
			//        System.out.println("Sent data to Offset " + recMetadata.offset() + " in Partition " + recMetadata.partition());
			//    }
			//});
			//System.out.println("send-Sync");
		}
		kafkaProducer.flush();
		kafkaProducer.close();
		System.out.println("close-Producer: " + new Date());
	}

	public static void main(String[] args) {
		TestProducer testProducer = new TestProducer();
		testProducer.start();
	}

}
