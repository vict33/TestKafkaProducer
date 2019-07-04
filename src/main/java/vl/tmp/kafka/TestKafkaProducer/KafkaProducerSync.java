package vl.tmp.kafka.TestKafkaProducer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerSync {
	// Kafka Java Producer and Consumer : Async (Callback)and Sync (get())
	// http://www.devinline.com/2018/09/kafka-java-sync-async-producer-and-consumer.html
	public static void main(String... args) throws Exception {
		if (args.length == 0) {
			doSyncProducer(5);
		} else {
			doSyncProducer(Integer.parseInt(args[0]));
		}
	}

	private static void doSyncProducer(int msgCount) {
		String topicName = "vlTest01";
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.80.51:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		long time = System.currentTimeMillis();
		Producer<Long, String> producer = new KafkaProducer<Long, String>(props);

		for (long i = time; i < time + msgCount; i++) {
			String message = "Message SYNC from " + topicName + "-" + i;
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topicName, i, message);

			// Synchronously send a record to a topic and returns RecordMetadata
			RecordMetadata outMetadata;
			try {
				outMetadata = producer.send(record).get();
				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), outMetadata.partition(), outMetadata.offset(), elapsedTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		producer.close();
		System.out.println("Message sent-Sync");

	}
}
