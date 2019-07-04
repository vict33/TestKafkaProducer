package vl.tmp.kafka.TestKafkaProducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TestProducerCallback implements Callback {

	private Integer messageKey;
	private String messageValue;
	private Long startTime;

	public TestProducerCallback(Integer messageKey, String messageValue, Long startTime) {
		this.messageKey = messageKey;
		this.messageValue = messageValue;
		this.startTime = startTime;
		System.out.println("Callback-constructor");
	}

	@Override
	public void onCompletion(RecordMetadata recMetadata, Exception e) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Callback-onCompletion");
		if (recMetadata != null) {
			System.out.println("message(" + messageKey + ", " + messageValue + ") sent to partition(" + recMetadata.partition() + "), " + "offset(" + recMetadata.offset() + ") in " + elapsedTime + " ms");
		} else {
			e.printStackTrace();
		}
	}

}
