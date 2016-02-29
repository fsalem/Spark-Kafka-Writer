package fi.aalto.spark.kafka;

import java.io.Serializable;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RDDKafkaWriter implements Serializable {

	private static final long serialVersionUID = 7374381310562055607L;
	private final KafkaProducerPool pool;

	public RDDKafkaWriter(final KafkaProducerPool pool) {
		this.pool = pool;

	}

	public void writeToKafka(String topic, String message) {
		KafkaProducer<String, String> producer = pool.borrowProducer();
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(
				topic, message);
		producer.send(record, new RecordAckCallback());
		pool.returnProducer(producer);
	}
}
