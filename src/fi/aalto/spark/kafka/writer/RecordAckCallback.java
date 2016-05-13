package fi.aalto.spark.kafka.writer;

import java.io.Serializable;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class RecordAckCallback implements Callback, Serializable {

	private static final long serialVersionUID = 5693320087523859178L;

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		// try {
		// BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
		// "SparkKafka.log"));
		// bufferedWriter.append("offset:" + metadata.offset() + ",parition:"
		// + metadata.partition() + ",Exception:"
		// + exception.getMessage() + "\n");
		// bufferedWriter.flush();
		// bufferedWriter.close();
		// } catch (Exception e) {
		// // TODO: handle exception
		// }

	}

}
