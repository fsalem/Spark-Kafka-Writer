package fi.aalto.spark.kafka.reader;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;

;

/**
 * 
 * @author Oleg Varaksin, Farouk Salem
 *
 */
public class KafkaConsumerPool implements Serializable {

	private static final long serialVersionUID = -1913028296093224674L;

	private transient ConcurrentLinkedQueue<KafkaConsumer<String, String>> pool;

	private ScheduledExecutorService executorService;

	private final Properties properties;

	private final int minIdle;

	/**
	 * Creates the pool.
	 *
	 * @param minIdle
	 *            minimum number of objects residing in the pool
	 */
	public KafkaConsumerPool(final int minIdle, final Properties properties) {
		// initialize pool
		this.properties = properties;
		this.minIdle = minIdle;
		initialize();

	}

	/**
	 * Creates the pool.
	 *
	 * @param minIdle
	 *            minimum number of objects residing in the pool
	 * @param maxIdle
	 *            maximum number of objects residing in the pool
	 * @param validationInterval
	 *            time in seconds for periodical checking of minIdle / maxIdle
	 *            conditions in a separate thread. When the number of objects is
	 *            less than minIdle, missing instances will be created. When the
	 *            number of objects is greater than maxIdle, too many instances
	 *            will be removed.
	 */
	public KafkaConsumerPool(final int minIdle, final int maxIdle,
			final long validationInterval, final Properties properties) {
		// initialize pool
		this.properties = properties;
		this.minIdle = minIdle;
		initialize();

		// check pool conditions in a separate thread
		executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				int size = pool.size();
				if (size < minIdle) {
					int sizeToBeAdded = minIdle - size;
					for (int i = 0; i < sizeToBeAdded; i++) {
						pool.add(createConsumer());
					}
				} else if (size > maxIdle) {
					int sizeToBeRemoved = size - maxIdle;
					for (int i = 0; i < sizeToBeRemoved; i++) {
						pool.poll();
					}
				}
			}
		}, validationInterval, validationInterval, TimeUnit.SECONDS);
	}

	/**
	 * Gets the next free object from the pool. If the pool doesn't contain any
	 * objects, a new object will be created and given to the caller of this
	 * method back.
	 *
	 * @return T borrowed object
	 */
	public synchronized KafkaConsumer<String, String> borrowConsumer() {
		if (pool == null)
			initialize();
		KafkaConsumer<String, String> object;
		if ((object = pool.poll()) == null) {
			object = createConsumer();
		}

		return object;
	}

	/**
	 * Returns object back to the pool.
	 *
	 * @param object
	 *            object to be returned
	 */
	public void returnConsumer(KafkaConsumer<String, String> Consumer) {
		if (Consumer == null) {
			return;
		}

		this.pool.offer(Consumer);
	}

	/**
	 * Shutdown this pool.
	 */
	public void shutdown() {
		if (executorService != null) {
			KafkaConsumer<String, String> Consumer;
			while ((Consumer = pool.poll()) != null) {
				Consumer.close();
			}
			executorService.shutdown();
		}
	}

	/**
	 * Creates a new Consumer.
	 *
	 * @return T new object
	 */
	private KafkaConsumer<String, String> createConsumer() {
		KafkaConsumer<String, String> Consumer = new KafkaConsumer<>(properties);
		return Consumer;
	}

	private void initialize() {
		pool = new ConcurrentLinkedQueue<KafkaConsumer<String, String>>();

		for (int i = 0; i < minIdle; i++) {
			pool.add(createConsumer());
		}
	}
}
