package fi.aalto.spark.kafka;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

public class JavaRDDKafkaWriter implements Serializable, VoidFunction<JavaRDD<String>> {

	private static final long serialVersionUID = -865193912367180261L;
	private final KafkaProducerPool pool;
	private final String topic;

	public JavaRDDKafkaWriter(final KafkaProducerPool pool, String topic) {
		this.pool = pool;
		this.topic = topic;
	}

	@Override
	public void call(JavaRDD<String> rdd) throws Exception {
		rdd.foreachPartition(new PartitionVoidFunction(
				new RDDKafkaWriter(pool), topic));
		
	}

	private class PartitionVoidFunction implements
			VoidFunction<Iterator<String>> {

		private static final long serialVersionUID = 8726871215617446598L;
		private final RDDKafkaWriter kafkaWriter;
		private final String topic;

		public PartitionVoidFunction(RDDKafkaWriter kafkaWriter, String topic) {
			this.kafkaWriter = kafkaWriter;
			this.topic = topic;
		}

		@Override
		public void call(Iterator<String> iterator) throws Exception {
			while (iterator.hasNext()) {
				kafkaWriter.writeToKafka(topic, iterator.next());
			}
		}
	}
}
