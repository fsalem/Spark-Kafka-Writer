package fi.aalto.spark.kafka.writer;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.JavaDStream;

import scala.Tuple2;

public class JavaDStreamKafkaWriter implements Serializable {

	private static final long serialVersionUID = 3934800110130868334L;
	private final KafkaProducerPool pool;
	private final JavaDStream<Tuple2<String, String>> javaDStream;
	private final String topic;
	private final Boolean kafkaAsync;

	public JavaDStreamKafkaWriter(final KafkaProducerPool pool,
			final JavaDStream<Tuple2<String, String>> javaDStream, final String topic, final Boolean kafkaAsync) {
		this.pool = pool;
		this.javaDStream = javaDStream;
		this.topic = topic;
		this.kafkaAsync = kafkaAsync;
	}

	public void writeToKafka() {
		// String tempTopic=topic.concat(UUID.randomUUID().toString());
		String tempTopic = topic;
		javaDStream.foreachRDD(new JavaRDDKafkaWriter(pool, tempTopic,kafkaAsync));
	}
}
