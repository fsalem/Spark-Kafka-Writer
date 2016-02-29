package fi.aalto.spark.kafka;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.streaming.api.java.JavaDStream;

public class JavaDStreamKafkaWriterFactory implements Serializable {

	private static final long serialVersionUID = -3309712415591664249L;

	public static JavaDStreamKafkaWriter getKafkaWriter(
			JavaDStream<String> javaDStream, Properties producerConfig) {
		return new JavaDStreamKafkaWriter(new KafkaProducerPool(3,
				producerConfig), javaDStream);
	}
}
