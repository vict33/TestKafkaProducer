package vl.tmp.kafka.TestKafkaProducer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerAsync {
	// Kafka Java Producer and Consumer : Async (Callback)and Sync (get())
	// http://www.devinline.com/2018/09/kafka-java-sync-async-producer-and-consumer.html
	public static void main(String... args) throws Exception {
		if (args.length == 0) {
			doAsyncProducer(5);
		} else {
			doAsyncProducer(Integer.parseInt(args[0]));
		}
	}

	private static void doAsyncProducer(final int sendMessageCount) throws InterruptedException {
		String topicName = "vlTest01";
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.80.51:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		long time = System.currentTimeMillis();
		Producer<Long, String> producer = new KafkaProducer<Long, String>(props);

		final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);
		try {
			for (long index = time; index < time + sendMessageCount; index++) {
				String message = "Message ASYNC from " + topicName + "-" + index;
				final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topicName, index, message);
				producer.send(record, (metadata, exception) -> {
					long elapsedTime = System.currentTimeMillis() - time;
					if (metadata != null) {
						System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(),elapsedTime);
					} else {
						exception.printStackTrace();
					}
					countDownLatch.countDown();
				});
			}
			countDownLatch.await(25, TimeUnit.SECONDS);
		} finally {
			producer.flush();
			producer.close();
		}
		System.out.println("Message sent-Async");
	}

}
