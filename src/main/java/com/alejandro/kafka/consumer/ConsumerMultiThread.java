package com.alejandro.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alejandro.kafka.util.Constants;

public class ConsumerMultiThread extends Thread {

	private static final Logger log = LoggerFactory.getLogger(ConsumerMultiThread.class);

	private final KafkaConsumer<String, String> consumer;
	private final AtomicBoolean closed;

	public ConsumerMultiThread(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
		this.closed = new AtomicBoolean(false);
	}

	@Override
	public void run() {
		this.consumer.subscribe(Arrays.asList(Constants.TOPIC_NAME));
		try {
			while (!this.closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				records.forEach(r -> {
					log.info("offset {}, partition {}, key {}, value {}", r.offset(), r.partition(), r.key(),
							r.value());
				});
			}
		} catch (WakeupException e) {
			if (!this.closed.get()) {
				throw e;
			}
		} finally {
			this.consumer.close();
		}

	}

	public void shuthdown() {
		this.closed.set(true);
		this.consumer.wakeup();
	}

}
