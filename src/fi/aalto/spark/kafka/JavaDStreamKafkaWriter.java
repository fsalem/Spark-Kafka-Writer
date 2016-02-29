package fi.aalto.spark.kafka;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.JavaDStream;

public class JavaDStreamKafkaWriter implements Serializable {

	private static final long serialVersionUID = 3934800110130868334L;
	private final KafkaProducerPool pool;
	private final JavaDStream<String> javaDStream;

	public JavaDStreamKafkaWriter(final KafkaProducerPool pool,
			final JavaDStream<String> javaDStream) {
		this.pool = pool;
		this.javaDStream = javaDStream;
	}

	public void writeToKafka(String topic) {
		javaDStream.foreachRDD(new JavaRDDKafkaWriter(pool, topic));
	}
}
