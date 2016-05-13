package fi.aalto.spark.kafka.writer;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class JavaRDDKafkaWriter implements Serializable, VoidFunction<JavaRDD<Tuple2<String, String>>> {

	private static final long serialVersionUID = -865193912367180261L;
	private final KafkaProducerPool pool;
	private final String topic;
	private final Boolean kafkaAsync;

	public JavaRDDKafkaWriter(final KafkaProducerPool pool, String topic,Boolean kafkaAsync) {
		this.pool = pool;
		this.topic = topic;
		this.kafkaAsync = kafkaAsync;
	}

	@Override
	public void call(JavaRDD<Tuple2<String, String>> rdd) throws Exception {
		rdd.foreachPartition(new PartitionVoidFunction(
				new RDDKafkaWriter(pool,kafkaAsync), topic));
		
	}

	private class PartitionVoidFunction implements
			VoidFunction<Iterator<Tuple2<String, String>>> {

		private static final long serialVersionUID = 8726871215617446598L;
		private final RDDKafkaWriter kafkaWriter;
		private final String topic;

		public PartitionVoidFunction(RDDKafkaWriter kafkaWriter, String topic) {
			this.kafkaWriter = kafkaWriter;
			this.topic = topic;
		}

		@Override
		public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
			while (iterator.hasNext()) {
				kafkaWriter.writeToKafka(topic, iterator.next());
			}
		}
	}
}
