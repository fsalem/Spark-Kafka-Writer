package fi.aalto.spark;

import java.io.Serializable;

import org.apache.spark.scheduler.JobListener;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;

import fi.aalto.spark.kafka.reader.KafkaConsumerPool;
import fi.aalto.spark.kafka.writer.KafkaProducerPool;

public class SparkJobListener implements SparkListener, JobListener, Serializable {

	private final KafkaConsumerPool kafkaConsumerPool;
	private final KafkaProducerPool kafkaProducerPool;
	private final String topic;

	public SparkJobListener(final KafkaConsumerPool kafkaConsumerPool,
			final KafkaProducerPool kafkaProducerPool, final String topic) {
		this.kafkaConsumerPool = kafkaConsumerPool;
		this.kafkaProducerPool = kafkaProducerPool;
		this.topic = topic;
	}

	@Override
	public void onApplicationEnd(SparkListenerApplicationEnd arg0) {
		System.out.println("SparkJobListener -> onApplicationEnd -> closing all producers");
		kafkaProducerPool.closeAll();
	}

	@Override
	public void onApplicationStart(SparkListenerApplicationStart arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onBlockManagerAdded(SparkListenerBlockManagerAdded arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onBlockUpdated(SparkListenerBlockUpdated arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onExecutorAdded(SparkListenerExecutorAdded arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onExecutorRemoved(SparkListenerExecutorRemoved arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onJobEnd(SparkListenerJobEnd jobEnd) {
//		System.out.println("onJobEnd -> JobId: " + jobEnd.jobId());
//		KafkaConsumer<String, String> kafkaConsumer = kafkaConsumerPool.borrowConsumer();
//		Map<String, List<PartitionInfo>> map = kafkaConsumer.listTopics();
//		Set<String> topicNames = map.keySet();
//		for(String topicName:topicNames){
//			if (topicName.startsWith(topic) && !topicName.equals(topic)){
//				kafkaConsumer.subscribe(Arrays.asList(topicName));
//				kafkaConsumer.poll(timeout)
//			}
//		}
	}

	@Override
	public void onJobStart(SparkListenerJobStart jobStart) {
//		System.out.println("onJobStart -> JobId: " + jobStart.jobId());
//		jobStart.properties().put("JobID", jobStart.jobId());
	}

	@Override
	public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
//		System.out.println("onStageCompleted -> StageId: "
//				+ stageCompleted.stageInfo().stageId());

	}

	@Override
	public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTaskGettingResult(SparkListenerTaskGettingResult gettingResult) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTaskStart(SparkListenerTaskStart taskStart) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onUnpersistRDD(SparkListenerUnpersistRDD arg0) {
		// TODO Auto-generated method stub

	}

	/**
	 * JobListener methods
	 */
	@Override
	public void jobFailed(Exception arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void taskSucceeded(int arg0, Object arg1) {
		// TODO Auto-generated method stub
		
	}

}
