package com.alejandro.kafka.consumer;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alejandro.kafka.util.Constants;

public class MyConsumer {
	
	Properties props;
	
	private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);
	
	public MyConsumer() {
		this.props = new Properties();
		this.loadProperties();
	}
	
	public void start() {
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){
			consumer.subscribe(Arrays.asList(Constants.TOPIC_NAME));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				records.forEach(r -> {
					log.info("offset {}, partition {}, key {}, value {}", r.offset(), r.partition(), r.key(), r.value());
				});
			}
		}
	}
	
	private void loadProperties() {
		try {
			this.props.load(new FileReader("src/main/resources/consumer.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
